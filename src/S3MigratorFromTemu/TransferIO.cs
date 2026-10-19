using System.Buffers;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;

namespace S3MigratorFromTemu;

internal static class TransferIO
{
    public static string DisplayKey(string key) => JsonSerializer.Serialize(key);
    public static string Bytes(long length) => $"{length:N0} bytes";

    public static async Task<string> HashAsync(Stream stream, CancellationToken token) =>
        Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, token));

    public static async Task<bool> LocalMatchesAsync(string path, ObjectRecord entry, CancellationToken token)
    {
        if (!File.Exists(path) || new FileInfo(path).Length != entry.Length) return false;
        await using var file = OpenRead(path);
        return await HashAsync(file, token) == entry.Sha256;
    }

    public static FileStream OpenRead(string path) => new(path, FileMode.Open, FileAccess.Read, FileShare.Read,
        128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);

    public static async Task<string> DownloadAndHashAsync(Stream input, Stream output, string key, long expectedLength,
        CancellationToken token)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(128 * 1024);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var progress = Stopwatch.StartNew();
        long total = 0;
        try
        {
            int read;
            while ((read = await input.ReadAsync(buffer.AsMemory(), token)) != 0)
            {
                await output.WriteAsync(buffer.AsMemory(0, read), token);
                hash.AppendData(buffer, 0, read);
                total += read;
                if (total > expectedLength) throw new IOException("Downloaded object exceeds its advertised length.");
                if (progress.Elapsed.TotalSeconds >= 5)
                {
                    Console.WriteLine($"  downloading {DisplayKey(key)}: {Bytes(total)} / {Bytes(expectedLength)}");
                    progress.Restart();
                }
            }
            if (total != expectedLength) throw new IOException("Incomplete download: object length does not match the listing.");
            return Convert.ToHexStringLower(hash.GetHashAndReset());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
