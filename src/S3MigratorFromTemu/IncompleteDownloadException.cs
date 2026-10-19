namespace S3MigratorFromTemu;

internal sealed class IncompleteDownloadException(long failedCount, long savedCount, string reportPath)
    : IOException($"Download incomplete: {savedCount:N0} objects saved/reused, {failedCount:N0} missing or changed objects. See {reportPath}. Upload is blocked until the download completes.")
{
}
