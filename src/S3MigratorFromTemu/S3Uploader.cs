using System.Diagnostics;
using System.Globalization;
using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace S3MigratorFromTemu;

internal sealed record UploadItem(ObjectRecord Object, string? FilePath, bool PreserveMetadata = true);

internal sealed class S3Uploader(string bucket)
{
    private const long MiB = 1024 * 1024;
    private static readonly string[] PreservedHeaders =
    ["Content-Type", "Cache-Control", "Content-Disposition", "Content-Encoding", "Content-Language", "Expires"];

    public async Task UploadAsync(IAmazonS3 target, string region, bool overwrite,
        IAsyncEnumerable<UploadItem> files, CancellationToken token)
    {
        await EnsureBucketAsync(target, region, token);
        using var transfer = new TransferUtility(target, new TransferUtilityConfig
        {
            ConcurrentServiceRequests = 4,
            MinSizeBeforePartUpload = 16 * MiB
        });
        long uploaded = 0, skipped = 0;
        await foreach (var item in files.WithCancellation(token))
        {
            var entry = item.Object;
            await using Stream local = item.FilePath is null ? new MemoryStream(Array.Empty<byte>()) : TransferIO.OpenRead(item.FilePath);
            var hash = await TransferIO.HashAsync(local, token);
            if (local.Length != entry.Length || entry.Sha256.Length != 0 && hash != entry.Sha256)
                throw new IOException($"Local content changed for {TransferIO.DisplayKey(entry.Key)}. Select the directory again or resume the download.");
            entry.Sha256 = hash;
            local.Position = 0;

            var status = await CompareTargetAsync(target, entry, item.PreserveMetadata, token);
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
                BucketName = bucket,
                Key = entry.Key,
                InputStream = local,
                AutoCloseStream = false,
                AutoResetStreamPosition = true,
                ContentType = entry.Headers.GetValueOrDefault("Content-Type") ?? "application/octet-stream",
                PartSize = PartSize(entry.Length),
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
            if (await CompareTargetAsync(target, entry, item.PreserveMetadata, token) != TargetStatus.Matching)
                throw new IOException($"Destination verification failed for {TransferIO.DisplayKey(entry.Key)}. Local files are retained.");
            uploaded++;
        }
        Console.WriteLine($"Upload complete: {uploaded:N0} uploaded and verified, {skipped:N0} already matching.");
    }

    private async Task EnsureBucketAsync(IAmazonS3 target, string region, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            await EnsureBucketCoreAsync(target, region, timeout.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw new HttpRequestException("The target connection/bucket check timed out.");
        }
    }

    private async Task EnsureBucketCoreAsync(IAmazonS3 target, string region, CancellationToken token)
    {
        try
        {
            await target.GetBucketLocationAsync(new GetBucketLocationRequest { BucketName = bucket }, token);
            return;
        }
        catch (AmazonS3Exception exception) when (exception.StatusCode == HttpStatusCode.NotFound || exception.ErrorCode == "NoSuchBucket") { }
        try
        {
            await target.PutBucketAsync(new PutBucketRequest
            {
                BucketName = bucket,
                UseClientRegion = false,
                BucketRegionName = region == "us-east-1" ? null : region
            }, token);
            Console.WriteLine($"Created target bucket {bucket}.");
        }
        catch (AmazonS3Exception exception) when (exception.ErrorCode == "BucketAlreadyOwnedByYou") { }
    }

    private enum TargetStatus { Missing, Different, Matching }

    private async Task<TargetStatus> CompareTargetAsync(IAmazonS3 target, ObjectRecord expected, bool verifyMetadata,
        CancellationToken token)
    {
        try
        {
            using var response = await target.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = expected.Key }, token);
            if (response.ContentLength != expected.Length || verifyMetadata &&
                (response.MissingMeta > 0 || !MetadataMatches(ReadMetadata(response.Metadata), expected.Metadata) ||
                    !HeadersMatch(ReadHeaders(response.Headers), expected.Headers)))
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

    internal static Dictionary<string, string> ReadMetadata(MetadataCollection metadata)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in metadata.Keys)
            result[key.StartsWith("x-amz-meta-", StringComparison.OrdinalIgnoreCase) ? key[11..] : key] = metadata[key];
        return result;
    }

    internal static Dictionary<string, string> ReadHeaders(HeadersCollection headers)
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
