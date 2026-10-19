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
        long downloaded = 0, reused = 0, failed = 0, bytes = 0;
        string? continuation = null;
        const string errorReportRelativePath = "download-errors.jsonl";

        Console.WriteLine("Downloading all current objects (including folder markers)...");
        await using var errors = new StreamWriter(store.PrepareFile(errorReportRelativePath), append: false, new UTF8Encoding(false))
        {
            AutoFlush = true
        };
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
                    // Decode exactly once below; MinIO represents spaces as '+' and literal '+' as '%2B'.
                    Encoding = EncodingType.Url
                }, token);

                foreach (var item in page.S3Objects ?? [])
                {
                    token.ThrowIfCancellationRequested();
                    // URL decoding must handle both MinIO's '+' and S3's '%20' for spaces.
                    // Decoding '%2B' once preserves an actual plus in an object name.
                    var key = string.Equals(page.Encoding?.Value, "url", StringComparison.OrdinalIgnoreCase)
                        ? WebUtility.UrlDecode(item.Key) : item.Key;
                    if (string.IsNullOrEmpty(key) || item.Size is null or < 0 || string.IsNullOrEmpty(item.ETag))
                        throw new IOException("The source returned an object without a key, size, or ETag.");

                    try
                    {
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
                            Console.WriteLine($"  downloading {TransferIO.DisplayKey(key)} ({TransferIO.Bytes(item.Size.Value)})...");
                            entry = await DownloadObjectAsync(source, item, key, store.AllocateLocalPath(key, cached), token);
                            await store.SaveCachedAsync(entry, token);
                            downloaded++;
                            Console.WriteLine($"  saved {TransferIO.DisplayKey(key)} ({TransferIO.Bytes(entry.Length)})");
                        }
                        bytes = checked(bytes + entry.Length);
                        await pending.WriteLineAsync(JsonSerializer.Serialize(entry, Settings.Json).AsMemory(), token);
                    }
                    catch (AmazonS3Exception exception) when (IsMissingOrChangedObject(exception) && !token.IsCancellationRequested)
                    {
                        failed++;
                        var reason = exception.StatusCode == HttpStatusCode.PreconditionFailed || exception.ErrorCode == "PreconditionFailed"
                            ? "The object changed between listing and download."
                            : "The object was listed, but the server could not find the requested key.";
                        Console.Error.WriteLine($"  failed {TransferIO.DisplayKey(key)}: {exception.ErrorCode} (HTTP {(int)exception.StatusCode}). {reason} Continuing with the remaining objects.");
                        await errors.WriteLineAsync(JsonSerializer.Serialize(new
                        {
                            bucket = store.State.Bucket,
                            key,
                            listedKey = item.Key,
                            listingEncoding = page.Encoding?.Value,
                            errorCode = exception.ErrorCode,
                            httpStatus = (int)exception.StatusCode,
                            requestId = exception.RequestId,
                            reason
                        }, Settings.Json).AsMemory(), token);
                    }
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
        if (failed != 0)
            throw new IncompleteDownloadException(failed, downloaded + reused, store.Resolve(errorReportRelativePath));
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

    private static bool IsMissingOrChangedObject(AmazonS3Exception exception) =>
        exception.ErrorCode is "NoSuchKey" or "PreconditionFailed" ||
        exception.StatusCode == HttpStatusCode.PreconditionFailed ||
        exception.StatusCode == HttpStatusCode.NotFound && exception.ErrorCode != "NoSuchBucket";

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
