using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;

namespace S3MigratorFromTemu;

internal sealed class Migrator(MigrationStore store)
{
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
                    Metadata = S3Uploader.ReadMetadata(response.Metadata),
                    Headers = S3Uploader.ReadHeaders(response.Headers)
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
        await new S3Uploader(store.State.Bucket).UploadAsync(target, region, overwrite, ReadUploadItemsAsync(token), token);
    }

    private async IAsyncEnumerable<UploadItem> ReadUploadItemsAsync([EnumeratorCancellation] CancellationToken token)
    {
        await foreach (var entry in store.ReadManifestAsync(token))
            yield return new UploadItem(entry, store.Resolve(entry.LocalPath));
    }
}
