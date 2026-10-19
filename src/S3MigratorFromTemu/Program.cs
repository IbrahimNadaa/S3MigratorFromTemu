using Amazon.S3;

namespace S3MigratorFromTemu;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        using var cancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cancellation.Cancel();
        };

        try
        {
            var options = CommandLine.Parse(args);
            if (options.Help)
            {
                Console.WriteLine(CommandLine.HelpText);
                return 0;
            }

            Console.WriteLine("s3 migrator from temu");
            Console.WriteLine();
            var settings = await Settings.LoadAsync(options, cancellation.Token);
            using var store = new MigrationStore(settings.WorkDirectory, options.Command != "upload");

            if (options.Command is "download" or "migrate")
                store.Initialize(settings.Bucket!, settings.Source!);
            else
                store.RequireCompletedSnapshot(settings.Bucket);

            if (settings.Target is not null &&
                string.Equals(settings.Target.Url, store.State.SourceUrl, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Source and target must be different S3 servers.");

            Console.WriteLine($"Bucket: {store.State.Bucket}");
            Console.WriteLine($"Local files: {store.Root}");
            if (settings.Target is not null)
            {
                Console.WriteLine($"Target: {settings.Target.Url}");
                Console.WriteLine(options.Overwrite
                    ? "Existing target keys may be replaced (--overwrite)."
                    : "Matching target objects are skipped; different existing objects stop the upload.");
            }

            var migrator = new Migrator(store);
            if (settings.Source is not null)
            {
                using var source = settings.Source.CreateClient();
                await migrator.DownloadAsync(source, cancellation.Token);
            }
            if (settings.Target is not null)
            {
                using var target = settings.Target.CreateClient();
                await migrator.UploadAsync(target, settings.Target.Region, options.Overwrite, cancellation.Token);
            }

            Console.WriteLine("Done. Local files and the manifest have been retained.");
            return 0;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            Console.Error.WriteLine("Cancelled. Run the same command again to resume.");
            return 130;
        }
        catch (AmazonS3Exception exception)
        {
            // Report service codes, never dump requests or credential-bearing exception data.
            Console.Error.WriteLine($"S3 error: {exception.ErrorCode} (HTTP {(int)exception.StatusCode}).");
            Console.Error.WriteLine("Check the endpoint, region, credentials, permissions, and server compatibility.");
            return 1;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"Error: {exception.Message}");
            return 1;
        }
    }
}
