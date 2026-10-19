using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace S3MigratorFromTemu;

internal sealed class Migrator(MigrationStore store)
{
    private const long MiB = 1024 * 1024;
    private static readonly string[] PreservedHeaders =
    ["Content-Type", "Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires"];

    public async Task DownloadAsync(IAmazonS3 source, CancellationToken token)
    {
        store.State.SnapshotComplete = false;
        await store.SaveStateAsync(token);
        long downloaded = 0, reused = 0, bytes = 0;
        string? continuation = null;

        Console.WriteLine("Downloading all current objects (including folder markers)...");
        await using (var pending = new StreamWriter(store.PrepareFile("manifest.pending.jsonl"), append: false, new UTF8Encoding(false)))
        {
            do
            {
                var page = await source.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = store.State.Bucket,
                    MaxKeys = 1000,
                    ContinuationToken = continuation,
                    // Explicit URL encoding permits keys that XML 1.0 cannot represent.
                    // This SDK returns explicitly requested URL encoding without decoding it.
                    Encoding = EncodingType.Url
                }, token);

                foreach (var item in page.S3Objects ?? [])
                {
                    token.ThrowIfCancellationRequested();
                    var key = page.Encoding?.Value == "url" ? Uri.UnescapeDataString(item.Key) : item.Key;
                    if (string.IsNullOrEmpty(key) || item.Size is null or < 0 || string.IsNullOrEmpty(item.ETag))
                        throw new IOException("The source returned an object without a key, size, or ETag.");

                    var cached = await store.ReadCachedAsync(key, token);
                    ObjectRecord entry;
                    if (cached is not null && cached.Length == item.Size && cached.SourceETag == item.ETag &&
                        cached.SourceLastModifiedUtc == item.LastModified?.ToUniversalTime() &&
                        await TransferIO.LocalMatchesAsync(store.Resolve(cached.LocalPath), cached, token))
                    {
                        entry = cached;
                        reused++;
                        Console.WriteLine($"  reused {TransferIO.DisplayKey(key)} ({TransferIO.Bytes(entry.Length)})");
                    }
                    else
                    {
                        entry = await DownloadObjectAsync(source, item, key, store.AllocateLocalPath(key, cached), token);
                        await store.SaveCachedAsync(entry, token);
                        downloaded++;
                        Console.WriteLine($"  saved {TransferIO.DisplayKey(key)} ({TransferIO.Bytes(entry.Length)})");
                    }
                    bytes = checked(bytes + entry.Length);
                    await pending.WriteLineAsync(JsonSerializer.Serialize(entry, Settings.Json).AsMemory(), token);
                }

                if (page.IsTruncated is null)
                    throw new IOException("The source did not indicate whether its object listing was complete.");
                if (page.IsTruncated != true) break;
                var next = page.NextContinuationToken;
                if (string.IsNullOrEmpty(next) || next == continuation)
                    throw new IOException("The source returned an invalid pagination token; the snapshot is incomplete.");
                continuation = next;
            } while (true);
            await pending.FlushAsync(token);
        }

        token.ThrowIfCancellationRequested();
        File.Move(store.PendingManifestPath, store.ManifestPath, overwrite: true);
        await using (var manifest = TransferIO.OpenRead(store.ManifestPath))
            store.State.ManifestSha256 = await TransferIO.HashAsync(manifest, token);
        store.State.ObjectCount = downloaded + reused;
        store.State.TotalBytes = bytes;
        store.State.SnapshotComplete = true;
        await store.SaveStateAsync(token);
        Console.WriteLine($"Download complete: {downloaded:N0} downloaded, {reused:N0} reused, {TransferIO.Bytes(bytes)} total.");
    }

    private async Task<ObjectRecord> DownloadObjectAsync(IAmazonS3 source, S3Object listed, string key,
        string localPath, CancellationToken token)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var response = await source.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = store.State.Bucket,
                    Key = key,
                    EtagToMatch = listed.ETag
                }, token);
                if (response.ContentLength != listed.Size || response.ETag != listed.ETag)
                    throw new IOException("The source object changed during download. Run download again after source writes stop.");
                if (response.MissingMeta > 0)
                    throw new IOException("The source contains metadata that cannot be retrieved through the S3 REST API.");

                var entry = new ObjectRecord
                {
                    Key = key,
                    LocalPath = localPath,
                    Length = listed.Size!.Value,
                    SourceETag = response.ETag,
                    SourceLastModifiedUtc = response.LastModified?.ToUniversalTime(),
                    Metadata = ReadMetadata(response.Metadata),
                    Headers = ReadHeaders(response.Headers)
                };
                var partialRelative = $"partials/{MigrationStore.ObjectId(key)}.part";
                var partial = store.PrepareFile(partialRelative);
                await using (var file = new FileStream(partial, FileMode.Create, FileAccess.Write, FileShare.None,
                    128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    entry.Sha256 = await TransferIO.DownloadAndHashAsync(response.ResponseStream, file, key, entry.Length, token);
                    await file.FlushAsync(token);
                }
                token.ThrowIfCancellationRequested();
                var destination = store.PrepareFile(localPath);
                File.Move(store.Resolve(partialRelative), destination, overwrite: true);
                return entry;
            }
            catch (Exception exception) when (attempt < 3 && IsRetryableDownload(exception) && !token.IsCancellationRequested)
            {
                Console.Error.WriteLine($"  retrying download {TransferIO.DisplayKey(key)} (attempt {attempt + 1}/3)...");
                await Task.Delay(TimeSpan.FromSeconds(attempt * 2), token);
            }
        }
    }

    private static bool IsRetryableDownload(Exception exception) => exception is IOException or HttpRequestException ||
        exception is AmazonS3Exception s3 && ((int)s3.StatusCode >= 500 || s3.StatusCode == HttpStatusCode.RequestTimeout);

    public async Task UploadAsync(IAmazonS3 target, string region, bool overwrite, CancellationToken token)
    {
        store.RequireCompletedSnapshot();
        await using (var manifest = TransferIO.OpenRead(store.ManifestPath))
            if (await TransferIO.HashAsync(manifest, token) != store.State.ManifestSha256)
                throw new IOException("The completed manifest was changed or truncated. Run download again.");
        long objectCount = 0, bytes = 0;
        // Inspect the manifest before any target writes, including an empty bucket snapshot.
        var keys = new HashSet<string>(StringComparer.Ordinal);
        await foreach (var entry in store.ReadManifestAsync(token))
        {
            if (!keys.Add(entry.Key)) throw new IOException("The manifest contains a duplicate S3 key.");
            var path = store.Resolve(entry.LocalPath);
            if (!File.Exists(path) || new FileInfo(path).Length != entry.Length)
                throw new IOException($"Missing or changed local file for {TransferIO.DisplayKey(entry.Key)}. Run download again.");
            objectCount++;
            bytes = checked(bytes + entry.Length);
        }
        keys.Clear();
        keys.TrimExcess();
        if (objectCount != store.State.ObjectCount || bytes != store.State.TotalBytes)
            throw new IOException("The manifest totals do not match the completed snapshot. Run download again.");

        Console.WriteLine($"Uploading {objectCount:N0} objects ({TransferIO.Bytes(bytes)}) to the same bucket name...");
        await EnsureBucketAsync(target, region, token);
        using var transfer = new TransferUtility(target, new TransferUtilityConfig
        {
            ConcurrentServiceRequests = 4,
            MinSizeBeforePartUpload = 16 * MiB
        });
        long uploaded = 0, skipped = 0;
        await foreach (var entry in store.ReadManifestAsync(token))
        {
            var path = store.Resolve(entry.LocalPath);
            await using var local = TransferIO.OpenRead(path);
            if (local.Length != entry.Length || await TransferIO.HashAsync(local, token) != entry.Sha256)
                throw new IOException($"Local content changed for {TransferIO.DisplayKey(entry.Key)}. Run download again.");
            local.Position = 0;

            var status = await CompareTargetAsync(target, entry, token);
            if (status == TargetStatus.Matching)
            {
                skipped++;
                Console.WriteLine($"  verified existing {TransferIO.DisplayKey(entry.Key)}");
                continue;
            }
            if (status == TargetStatus.Different && !overwrite)
                throw new IOException($"Target object {TransferIO.DisplayKey(entry.Key)} already exists with different content or metadata. Use --overwrite to replace it.");

            var request = new TransferUtilityUploadRequest
            {
                BucketName = store.State.Bucket,
                Key = entry.Key, // Never reconstruct an S3 key from a local filesystem path.
                InputStream = local,
                AutoCloseStream = false,
                AutoResetStreamPosition = true,
                ContentType = entry.Headers.GetValueOrDefault("Content-Type") ?? "application/octet-stream",
                PartSize = PartSize(entry.Length),
                // Protect against another writer creating an object after the existence check.
                IfNoneMatch = overwrite ? null : "*"
            };
            foreach (var pair in entry.Metadata) request.Metadata.Add(pair.Key, pair.Value);
            foreach (var pair in entry.Headers)
            {
                if (pair.Key.Equals("Expires", StringComparison.OrdinalIgnoreCase)) request.Expires = pair.Value;
                else request.Headers[pair.Key] = pair.Value;
            }
            var progress = Stopwatch.StartNew();
            request.UploadProgressEvent += (_, args) =>
            {
                lock (progress)
                {
                    if (progress.Elapsed.TotalSeconds < 5) return;
                    Console.WriteLine($"  uploading {TransferIO.DisplayKey(entry.Key)}: {TransferIO.Bytes(args.TransferredBytes)} / {TransferIO.Bytes(entry.Length)}");
                    progress.Restart();
                }
            };

            Console.WriteLine($"  uploading {TransferIO.DisplayKey(entry.Key)} ({TransferIO.Bytes(entry.Length)})");
            await transfer.UploadAsync(request, token);
            Console.WriteLine($"  checking destination SHA-256 for {TransferIO.DisplayKey(entry.Key)}...");
            if (await CompareTargetAsync(target, entry, token) != TargetStatus.Matching)
                throw new IOException($"Destination verification failed for {TransferIO.DisplayKey(entry.Key)}. Local files are retained.");
            uploaded++;
        }
        Console.WriteLine($"Upload complete: {uploaded:N0} uploaded and verified, {skipped:N0} already matching.");
    }

    private async Task EnsureBucketAsync(IAmazonS3 target, string region, CancellationToken token)
    {
        try
        {
            await target.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = store.State.Bucket }, token);
            return;
        }
        catch (AmazonS3Exception exception) when (exception.StatusCode == HttpStatusCode.NotFound || exception.ErrorCode == "NoSuchBucket")
        {
            // Access denied is intentionally not treated as a missing bucket.
        }
        try
        {
            await target.PutBucketAsync(new PutBucketRequest
            {
                BucketName = store.State.Bucket,
                UseClientRegion = false,
                BucketRegionName = region == "us-east-1" ? null : region
            }, token);
            Console.WriteLine($"Created target bucket {store.State.Bucket}.");
        }
        catch (AmazonS3Exception exception) when (exception.ErrorCode == "BucketAlreadyOwnedByYou")
        {
            // A concurrent process created the intended bucket for this account.
        }
    }

    private enum TargetStatus { Missing, Different, Matching }

    private async Task<TargetStatus> CompareTargetAsync(IAmazonS3 target, ObjectRecord expected, CancellationToken token)
    {
        try
        {
            using var response = await target.GetObjectAsync(new GetObjectRequest
            {
                BucketName = store.State.Bucket,
                Key = expected.Key
            }, token);
            if (response.ContentLength != expected.Length || response.MissingMeta > 0 ||
                !MetadataMatches(ReadMetadata(response.Metadata), expected.Metadata) ||
                !HeadersMatch(ReadHeaders(response.Headers), expected.Headers))
                return TargetStatus.Different;
            return await TransferIO.HashAsync(response.ResponseStream, token) == expected.Sha256
                ? TargetStatus.Matching : TargetStatus.Different;
        }
        catch (AmazonS3Exception exception) when (exception.ErrorCode == "NoSuchKey" ||
            exception.StatusCode == HttpStatusCode.NotFound && exception.ErrorCode != "NoSuchBucket")
        {
            return TargetStatus.Missing;
        }
    }

    private static Dictionary<string, string> ReadMetadata(MetadataCollection metadata)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in metadata.Keys)
            result[key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase) ? key[11..] : key] = metadata[key];
        return result;
    }

    private static Dictionary<string, string> ReadHeaders(HeadersCollection headers)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in PreservedHeaders)
            if (!string.IsNullOrEmpty(headers[name])) result[name] = headers[name];
        return result;
    }

    private static bool MetadataMatches(Dictionary<string, string> actual, Dictionary<string, string> expected) =>
        actual.Count == expected.Count && expected.All(pair => actual.TryGetValue(pair.Key, out var value) && value == pair.Value);

    private static bool HeadersMatch(Dictionary<string, string> actual, Dictionary<string, string> expected)
    {
        foreach (var name in PreservedHeaders)
        {
            var wanted = expected.GetValueOrDefault(name);
            if (name == "Content-Type" && string.IsNullOrEmpty(wanted)) wanted = "application/octet-stream";
            var found = actual.GetValueOrDefault(name);
            if (string.IsNullOrEmpty(wanted) && string.IsNullOrEmpty(found)) continue;
            if (wanted is null || found is null || !HeaderValueMatches(name, found, wanted)) return false;
        }
        return true;
    }

    private static bool HeaderValueMatches(string name, string actual, string expected)
    {
        if (name.Equals("Expires", StringComparison.OrdinalIgnoreCase) &&
            DateTimeOffset.TryParse(actual, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var first) &&
            DateTimeOffset.TryParse(expected, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var second))
            return first == second;
        return actual == expected;
    }

    private static long PartSize(long length)
    {
        var size = Math.Max(16 * MiB, ((length / 9999 + MiB - 1) / MiB) * MiB);
        if (size > 5L * 1024 * MiB) throw new IOException("Object exceeds the supported multipart size limit.");
        return size;
    }
}
