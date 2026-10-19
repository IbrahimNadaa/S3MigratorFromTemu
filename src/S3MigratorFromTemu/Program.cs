using System.Text.Json;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace S3MigratorFromTemu;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellation.Cancel(); };
        try
        {
            var options = CommandLine.Parse(args);
            if (options.Help) { Console.WriteLine(CommandLine.HelpText); return 0; }
            Console.WriteLine("s3 migrator from temu");
            Console.WriteLine();
            options.SelectCommand(cancellation.Token);
            var settings = await Settings.LoadAsync(options, cancellation.Token);

            if (options.Command is "download" or "migrate")
            {
                settings.WorkDirectory = await DownloadAsync(settings, cancellation.Token);
                if (options.Command == "download")
                {
                    Console.WriteLine($"Saved to: {settings.WorkDirectory}");
                    Console.WriteLine("Download finished. You can close the app and choose Upload from a local directory later.");
                    return 0;
                }
                settings.UploadDirectory = settings.WorkDirectory;
            }
            await UploadAsync(settings, options, cancellation.Token);
            Console.WriteLine("Done. Local files have been retained.");
            return 0;
        }
        catch (OperationCanceledException exception) when (cancellation.IsCancellationRequested || exception.CancellationToken.IsCancellationRequested)
        {
            Input.Error("Cancelled. Local files are retained; run the app again to resume.");
            return 130;
        }
        catch (Exception exception)
        {
            Explain(exception);
            return 1;
        }
    }

    private static async Task<string> DownloadAsync(Settings settings, CancellationToken token)
    {
        while (true)
        {
            try
            {
                using var source = settings.Source!.CreateClient();
                // Validate the supplied server, credentials, and bucket before binding a local workspace.
                Console.WriteLine("Checking the source connection and bucket...");
                await CheckSourceAsync(source, settings.Bucket!, token);
                using var store = OpenDownloadStore(settings, token);
                Console.WriteLine($"Bucket: {store.State.Bucket}");
                Console.WriteLine($"Saving to: {store.Root}");
                await new Migrator(store).DownloadAsync(source, token);
                return store.Root;
            }
            catch (IncompleteDownloadException exception) when (Input.Interactive && !token.IsCancellationRequested)
            {
                // An object-level 404/412 is not a failure of the connection or credentials.
                Input.Error(exception.Message);
                var choice = Input.Choice("Some source objects could not be saved:", token,
                    ("retry", "List the bucket again and retry (reuse completed downloads)"),
                    ("cancel", "Stop and keep all downloaded files and the error report"));
                if (choice == "cancel") Cancel();
            }
            catch (Exception exception) when (Input.Interactive && !token.IsCancellationRequested && IsConnectionProblem(exception))
            {
                Explain(exception);
                settings.CorrectConnection(settings.Source!, "source", allowBucketChange: true, forbiddenUrl: null, token);
            }
            catch (Exception exception) when (Input.Interactive && !token.IsCancellationRequested && IsLocalProblem(exception))
            {
                Explain(exception);
                var choice = Input.Choice("The download has not completed. Choose how to continue:", token,
                    ("retry", "Retry the download"), ("directory", "Choose a different save directory"),
                    ("connection", "Correct source connection details"), ("cancel", "Cancel"));
                if (choice == "directory") settings.WorkDirectory = ReadDirectory("Directory to save the files", false, token);
                else if (choice == "connection")
                    settings.CorrectConnection(settings.Source!, "source", true, null, token);
                else if (choice == "cancel") Cancel();
            }
        }
    }

    private static async Task CheckSourceAsync(IAmazonS3 source, string bucket, CancellationToken token)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        try
        {
            await source.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = bucket,
                MaxKeys = 1,
                Encoding = EncodingType.Url
            }, timeout.Token);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw new HttpRequestException("The source connection check timed out.");
        }
    }

    private static MigrationStore OpenDownloadStore(Settings settings, CancellationToken token)
    {
        while (true)
        {
            MigrationStore? store = null;
            try
            {
                store = new MigrationStore(settings.WorkDirectory, allowCreate: true);
                store.Initialize(settings.Bucket!, settings.Source!);
                return store;
            }
            catch (Exception exception) when (IsLocalProblem(exception))
            {
                store?.Dispose();
                if (!Input.Interactive) throw;
                Explain(exception);
                settings.WorkDirectory = ReadDirectory("Directory to save the files (empty or matching snapshot)", false, token);
            }
        }
    }

    private static async Task UploadAsync(Settings settings, CommandLine options, CancellationToken token)
    {
        var targetConfigured = false;
        while (true)
        {
            MigrationStore? store = null;
            string? sourceUrl = null;
            try
            {
                var directory = Input.DirectoryPath(settings.UploadDirectory!, mustExist: true);
                DirectoryUpload? ordinary = null;
                (long Objects, long Bytes) totals = default;
                if (IsSnapshotDirectory(directory))
                {
                    store = new MigrationStore(directory, allowCreate: false);
                    store.RequireCompletedSnapshot();
                    sourceUrl = store.State.SourceUrl;
                    if (settings.Bucket is not null && settings.Bucket != store.State.Bucket)
                        Console.WriteLine($"Using the snapshot's original bucket name: {store.State.Bucket}.");
                    settings.Bucket = store.State.Bucket;
                    Console.WriteLine($"Completed snapshot: {directory}");
                }
                else
                {
                    RejectSnapshotSubdirectory(directory);
                    ordinary = new DirectoryUpload(directory);
                    Console.WriteLine("Inspecting the local directory...");
                    totals = ordinary.Inspect(token);
                    settings.Bucket = Input.Read("Target bucket name", token, initial: settings.Bucket, normalize: Input.Bucket);
                    Console.WriteLine($"Local directory: {directory}");
                }

                if (!targetConfigured)
                {
                    settings.ResolveTarget(options, sourceUrl, token);
                    targetConfigured = true;
                }
                else
                {
                    settings.Target!.Url = Input.Read("target S3 API URL", token, initial: settings.Target.Url,
                        normalize: value => EndpointSettings.ValidateUrl(value, sourceUrl));
                }
                Console.WriteLine($"Target: {settings.Target!.Url}");
                Console.WriteLine($"Bucket: {settings.Bucket}");
                Console.WriteLine(options.Overwrite
                    ? "Different existing target objects may be replaced (--overwrite)."
                    : "Matching target objects are skipped; different existing objects are protected.");
                using var target = settings.Target.CreateClient();
                if (store is not null)
                    await new Migrator(store).UploadAsync(target, settings.Target.Region, options.Overwrite, token);
                else
                {
                    Console.WriteLine($"Uploading {totals.Objects:N0} objects ({TransferIO.Bytes(totals.Bytes)})...");
                    await new S3Uploader(settings.Bucket!).UploadAsync(target, settings.Target.Region, options.Overwrite,
                        ordinary!.ReadAsync(token), token);
                }
                return;
            }
            catch (Exception exception) when (Input.Interactive && !token.IsCancellationRequested && IsConnectionProblem(exception))
            {
                Explain(exception);
                settings.CorrectConnection(settings.Target!, "target", allowBucketChange: store is null, sourceUrl, token);
            }
            catch (Exception exception) when (Input.Interactive && !token.IsCancellationRequested && IsLocalProblem(exception))
            {
                Explain(exception);
                var choices = new List<(string Value, string Label)>
                {
                    ("retry", "Retry the upload"),
                    ("directory", "Choose a different directory to upload")
                };
                if (targetConfigured) choices.Add(("connection", "Correct target connection details"));
                if (targetConfigured && !options.Overwrite) choices.Add(("overwrite", "Retry and allow replacing different target objects"));
                choices.Add(("cancel", "Cancel"));
                var choice = Input.Choice("The upload has not completed. Choose how to continue:", token, choices.ToArray());
                if (choice == "directory") settings.UploadDirectory = ReadDirectory("Directory to upload", true, token);
                else if (choice == "connection") settings.CorrectConnection(settings.Target!, "target", store is null, sourceUrl, token);
                else if (choice == "overwrite") options.EnableOverwrite();
                else if (choice == "cancel") Cancel();
            }
            finally
            {
                store?.Dispose();
            }
        }
    }

    private static bool IsSnapshotDirectory(string directory) =>
        new[] { "workspace.json", "workspace.json.tmp", "manifest.jsonl", "manifest.pending.jsonl" }
            .Any(name => File.Exists(Path.Combine(directory, name)));

    private static void RejectSnapshotSubdirectory(string directory)
    {
        var name = Path.GetFileName(Path.TrimEndingDirectorySeparator(directory));
        var parent = Directory.GetParent(directory)?.FullName;
        if (parent is not null && (name is "files" or "special" or "cache" or "partials") && IsSnapshotDirectory(parent))
            throw new IOException($"Select the entire downloaded snapshot directory: {parent}. This preserves the original S3 keys.");
    }

    private static string ReadDirectory(string label, bool mustExist, CancellationToken token) =>
        Input.Read(label, token, normalize: value => Input.DirectoryPath(value, mustExist));

    private static bool IsConnectionProblem(Exception exception) =>
        exception is AmazonS3Exception or AmazonClientException or HttpRequestException or TaskCanceledException or System.Xml.XmlException;

    private static bool IsLocalProblem(Exception exception) => Input.IsInputError(exception) || exception is JsonException;

    private static void Cancel() => throw new OperationCanceledException(new CancellationToken(canceled: true));

    private static void Explain(Exception exception)
    {
        if (exception is AmazonS3Exception s3)
            Input.Error($"S3 error: {s3.ErrorCode} (HTTP {(int)s3.StatusCode}). Check the API URL, bucket, region, credentials, and permissions.");
        else if (IsConnectionProblem(exception))
            Input.Error("Could not complete the S3 connection/request. Check the API URL, API port, region, credentials, and TLS certificate.");
        else
            Input.Error($"Error: {exception.Message}");
    }
}
