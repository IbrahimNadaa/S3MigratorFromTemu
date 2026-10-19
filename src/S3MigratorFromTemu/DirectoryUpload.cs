using System.Runtime.CompilerServices;
using System.Text;

namespace S3MigratorFromTemu;

internal sealed class DirectoryUpload(string root)
{
    public (long Objects, long Bytes) Inspect(CancellationToken token)
    {
        long count = 0, bytes = 0;
        foreach (var item in Enumerate(token))
        {
            if (item.FilePath is not null)
            {
                using var readable = TransferIO.OpenRead(item.FilePath);
                if (readable.Length != item.Object.Length) throw new IOException("The directory changed while it was being inspected.");
            }
            count++;
            bytes = checked(bytes + item.Object.Length);
        }
        return (count, bytes);
    }

    public async IAsyncEnumerable<UploadItem> ReadAsync([EnumeratorCancellation] CancellationToken token)
    {
        foreach (var item in Enumerate(token))
        {
            yield return item;
            await Task.CompletedTask;
        }
    }

    private IEnumerable<UploadItem> Enumerate(CancellationToken token)
    {
        Input.DirectoryPath(root, mustExist: true);
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.TryPop(out var directory))
        {
            token.ThrowIfCancellationRequested();
            RejectLink(directory);
            var empty = true;
            foreach (var path in Directory.EnumerateFileSystemEntries(directory))
            {
                token.ThrowIfCancellationRequested();
                empty = false;
                RejectLink(path);
                if (Directory.Exists(path))
                {
                    pending.Push(path);
                    continue;
                }
                var key = RelativeKey(path);
                yield return new UploadItem(new ObjectRecord
                {
                    Key = key,
                    Length = new FileInfo(path).Length,
                    Headers = new Dictionary<string, string> { ["Content-Type"] = "application/octet-stream" }
                }, path, PreserveMetadata: false);
            }
            if (empty && directory != root)
                yield return new UploadItem(new ObjectRecord { Key = RelativeKey(directory) + "/", Length = 0 }, null,
                    PreserveMetadata: false);
        }
    }

    private string RelativeKey(string path)
    {
        var relative = Path.GetRelativePath(root, path);
        if (Path.IsPathRooted(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new IOException("A selected file is outside the upload directory.");
        var key = relative.Replace(Path.DirectorySeparatorChar, '/');
        // Reserve one byte for the slash of an empty-directory marker.
        if (Encoding.UTF8.GetByteCount(key) > (Directory.Exists(path) ? 1023 : 1024))
            throw new IOException($"A relative filename is too long for an S3 key: {TransferIO.DisplayKey(key)}.");
        return key;
    }

    private static void RejectLink(string path)
    {
        if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            throw new IOException($"The upload directory contains a symbolic link or junction: {path}. Choose a folder containing ordinary files/directories.");
    }
}
