using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace S3MigratorFromTemu;

internal sealed class WorkspaceState
{
    public int SchemaVersion { get; set; } = 1;
    public string Bucket { get; set; } = "";
    public string SourceUrl { get; set; } = "";
    public string SourceRegion { get; set; } = "";
    public bool SnapshotComplete { get; set; }
    public long ObjectCount { get; set; }
    public long TotalBytes { get; set; }
    public string ManifestSha256 { get; set; } = "";
}

internal sealed class ObjectRecord
{
    public string Key { get; set; } = "";
    public string LocalPath { get; set; } = "";
    public long Length { get; set; }
    public string SourceETag { get; set; } = "";
    public DateTime? SourceLastModifiedUtc { get; set; }
    public string Sha256 { get; set; } = "";
    public Dictionary<string, string> Metadata { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, string> Headers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

internal sealed class MigrationStore : IDisposable
{
    private readonly FileStream processLock;
    private readonly Dictionary<string, Dictionary<string, string?>> mirrorEntries = new(StringComparer.Ordinal);
    public string Root { get; }
    public WorkspaceState State { get; private set; } = null!;
    public string ManifestPath => Resolve("manifest.jsonl");
    public string PendingManifestPath => Resolve("manifest.pending.jsonl");

    public MigrationStore(string directory, bool allowCreate)
    {
        Root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
        if (!allowCreate && !Directory.Exists(Root))
            throw new IOException("No local snapshot exists. Run download first.");
        RejectLink(Root);
        Directory.CreateDirectory(Root);
        var lockPath = Resolve(".migration.lock");
        try
        {
            processLock = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException)
        {
            throw new IOException("Cannot lock the staging directory. Check permissions and close other migrator processes using it.");
        }
        try
        {
            var statePath = Resolve("workspace.json");
            if (File.Exists(statePath))
            {
                State = JsonSerializer.Deserialize<WorkspaceState>(File.ReadAllText(statePath), Settings.Json)
                    ?? throw new IOException("workspace.json is empty.");
                if (State.SchemaVersion != 1 || string.IsNullOrWhiteSpace(State.Bucket) ||
                    string.IsNullOrWhiteSpace(State.SourceUrl))
                    throw new IOException("Unsupported or invalid workspace.json.");
            }
            else if (!allowCreate)
            {
                throw new IOException("No workspace.json found. Run download first.");
            }
        }
        catch
        {
            processLock.Dispose();
            throw;
        }
    }

    public void Initialize(string bucket, EndpointSettings source)
    {
        if (!Regex.IsMatch(bucket, "^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", RegexOptions.CultureInvariant))
            throw new ArgumentException("Use a DNS-compatible S3 bucket name (3-63 lowercase letters, numbers, dots, or hyphens).");
        if (State is not null)
        {
            if (State.Bucket != bucket || State.SourceUrl != source.Url || State.SourceRegion != source.Region)
                throw new IOException("This staging directory belongs to another source/bucket/region. Choose a different --workdir.");
            return;
        }
        if (Directory.EnumerateFileSystemEntries(Root).Any(path => Path.GetFileName(path) != ".migration.lock"))
            throw new IOException("Choose an empty staging directory to keep migration state separate from other files.");
        State = new WorkspaceState { Bucket = bucket, SourceUrl = source.Url, SourceRegion = source.Region };
        SaveStateAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public void RequireCompletedSnapshot(string? requestedBucket = null)
    {
        if (State is null || !State.SnapshotComplete || !File.Exists(ManifestPath) || File.Exists(PendingManifestPath) ||
            State.ObjectCount < 0 || State.TotalBytes < 0 || State.ManifestSha256 is null ||
            !Regex.IsMatch(State.ManifestSha256, "^[0-9a-f]{64}$", RegexOptions.CultureInvariant))
            throw new IOException("The local download is incomplete. Run download again before uploading.");
        if (requestedBucket is not null && requestedBucket != State.Bucket)
            throw new ArgumentException("The requested bucket differs from the downloaded bucket. Bucket renaming is not supported.");
    }

    public Task SaveStateAsync(CancellationToken token) => WriteJsonAsync("workspace.json", State, token);

    public async Task<ObjectRecord?> ReadCachedAsync(string key, CancellationToken token)
    {
        var path = CachePath(key);
        if (!File.Exists(Resolve(path))) return null;
        var entry = JsonSerializer.Deserialize<ObjectRecord>(await File.ReadAllTextAsync(Resolve(path), token), Settings.Json)
            ?? throw new IOException("An object checkpoint is empty. Use a new staging directory.");
        Validate(entry);
        if (entry.Key != key) throw new IOException("Object checkpoint key mismatch.");
        return entry;
    }

    public Task SaveCachedAsync(ObjectRecord entry, CancellationToken token) => WriteJsonAsync(CachePath(entry.Key), entry, token);

    private static string CachePath(string key)
    {
        var id = ObjectId(key);
        return $"cache/{id[..2]}/{id}.json";
    }

    public string AllocateLocalPath(string key, ObjectRecord? cached)
    {
        if (cached is not null)
        {
            Validate(cached);
            RememberMirrorPath(cached.LocalPath);
            return cached.LocalPath;
        }
        var segments = key.Split('/');
        if (segments.All(IsPortableSegment))
        {
            var relative = "files/" + key;
            var candidate = Resolve(relative);
            if (candidate.Length <= 240 && CanUseMirrorPath(segments))
                return relative;
        }
        // The original S3 key always lives in the manifest. Hashes are only local names.
        var id = ObjectId(key);
        return $"special/{id[..2]}/{id}.bin";
    }

    private bool CanUseMirrorPath(string[] segments)
    {
        var parent = Resolve("files");
        if (File.Exists(parent)) return false;
        for (var index = 0; index < segments.Length; index++)
        {
            if (File.Exists(parent)) return false;
            if (!mirrorEntries.TryGetValue(parent, out var children))
            {
                children = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
                if (Directory.Exists(parent))
                    foreach (var child in Directory.EnumerateFileSystemEntries(parent))
                        if (!children.TryAdd(Path.GetFileName(child), child)) children[Path.GetFileName(child)] = null;
                mirrorEntries[parent] = children;
            }
            if (children.TryGetValue(segments[index], out var existing))
            {
                if (existing is null || Path.GetFileName(existing) != segments[index]) return false;
                RejectLink(existing);
                if (index == segments.Length - 1 || File.Exists(existing)) return false;
                parent = existing;
            }
            else
            {
                var next = Path.Combine(parent, segments[index]);
                children.Add(segments[index], next);
                parent = next;
            }
        }
        return true;
    }

    private void RememberMirrorPath(string relative)
    {
        if (!relative.StartsWith("files/", StringComparison.Ordinal)) return;
        var parent = Resolve("files");
        foreach (var segment in relative[6..].Split('/'))
        {
            var child = Path.Combine(parent, segment);
            if (mirrorEntries.TryGetValue(parent, out var children)) children.TryAdd(segment, child);
            parent = child;
        }
    }

    private static bool IsPortableSegment(string segment)
    {
        if (segment.Length is 0 or > 180 || Encoding.UTF8.GetByteCount(segment) > 180 ||
            segment is "." or ".." || segment.EndsWith('.') || segment.EndsWith(' '))
            return false;
        if (segment.Any(character => char.IsControl(character) || "<>:\"/\\|?*".Contains(character)))
            return false;
        var stem = segment.Split('.')[0];
        return !Regex.IsMatch(stem, "^(CON|PRN|AUX|NUL|CLOCK\\$|CONIN\\$|CONOUT\\$|COM[1-9¹²³]|LPT[1-9¹²³])$",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    public void Validate(ObjectRecord entry)
    {
        if (string.IsNullOrEmpty(entry.Key) || entry.Length < 0 || entry.Metadata is null || entry.Headers is null ||
            entry.Sha256 is null || !Regex.IsMatch(entry.Sha256, "^[0-9a-f]{64}$", RegexOptions.CultureInvariant) ||
            entry.LocalPath is null || !(entry.LocalPath.StartsWith("files/", StringComparison.Ordinal) ||
                entry.LocalPath.StartsWith("special/", StringComparison.Ordinal)))
            throw new IOException("Invalid object record in the migration manifest/checkpoint.");
        // Content files cannot target workspace state or escape the staging directory.
        var segments = entry.LocalPath.Split('/');
        if (segments.Any(segment => !IsPortableSegment(segment)))
            throw new IOException("Unsafe local path in the migration manifest/checkpoint.");
        Resolve(entry.LocalPath);
    }

    public async IAsyncEnumerable<ObjectRecord> ReadManifestAsync([EnumeratorCancellation] CancellationToken token)
    {
        RequireCompletedSnapshot();
        await foreach (var line in File.ReadLinesAsync(ManifestPath, token))
        {
            var entry = JsonSerializer.Deserialize<ObjectRecord>(line, Settings.Json)
                ?? throw new IOException("Empty manifest record.");
            Validate(entry);
            yield return entry;
        }
    }

    public string Resolve(string relative)
    {
        if (string.IsNullOrEmpty(relative) || Path.IsPathRooted(relative))
            throw new IOException("Expected a relative staging path.");
        var absolute = Path.GetFullPath(Path.Combine(Root, relative.Replace('/', Path.DirectorySeparatorChar)));
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (!absolute.StartsWith(Path.TrimEndingDirectorySeparator(Root) + Path.DirectorySeparatorChar, comparison))
            throw new IOException("A staging path escapes the working directory.");
        RejectLink(Root);
        var current = Root;
        foreach (var segment in Path.GetRelativePath(Root, absolute).Split(Path.DirectorySeparatorChar))
        {
            current = Path.Combine(current, segment);
            RejectLink(current);
        }
        return absolute;
    }

    public string PrepareFile(string relative)
    {
        var path = Resolve(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return Resolve(relative);
    }

    private async Task WriteJsonAsync<T>(string relative, T value, CancellationToken token)
    {
        var temporary = PrepareFile(relative + ".tmp");
        await File.WriteAllTextAsync(temporary, JsonSerializer.Serialize(value, Settings.Json), new UTF8Encoding(false), token);
        File.Move(Resolve(relative + ".tmp"), Resolve(relative), overwrite: true);
    }

    private static void RejectLink(string path)
    {
        try
        {
            if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
                throw new IOException("Staging files and directories must not contain symbolic links or junctions.");
        }
        catch (FileNotFoundException) { }
        catch (DirectoryNotFoundException) { }
    }

    public static string ObjectId(string key) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
    public void Dispose() => processLock.Dispose();
}
