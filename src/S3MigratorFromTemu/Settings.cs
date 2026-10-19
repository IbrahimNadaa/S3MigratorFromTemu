using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.Runtime;
using Amazon.S3;

namespace S3MigratorFromTemu;

internal sealed class Settings
{
    public string? Bucket { get; set; }
    public string WorkDirectory { get; set; } = "./s3-migration";
    public EndpointSettings? Source { get; set; }
    public EndpointSettings? Target { get; set; }

    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow
    };

    public static async Task<Settings> LoadAsync(CommandLine options, CancellationToken token)
    {
        var configPath = options.Get("config");
        var settings = configPath is null
            ? new Settings()
            : JsonSerializer.Deserialize<Settings>(await File.ReadAllTextAsync(configPath, token), Json)
                ?? throw new ArgumentException("The configuration file is empty.");

        settings.WorkDirectory = Value(options.Get("workdir"), Environment.GetEnvironmentVariable("S3M_WORKDIR"),
            settings.WorkDirectory) ?? "./s3-migration";
        settings.Bucket = Value(options.Get("bucket"), Environment.GetEnvironmentVariable("S3M_BUCKET"), settings.Bucket);
        if (options.Command != "upload")
        {
            settings.Bucket = Required(settings.Bucket, "Bucket name");
            settings.Source = ResolveEndpoint(options, "source", settings.Source);
        }
        else
        {
            settings.Source = null;
        }
        settings.Target = options.Command == "download" ? null : ResolveEndpoint(options, "target", settings.Target);
        return settings;
    }

    private static EndpointSettings ResolveEndpoint(CommandLine options, string side, EndpointSettings? configured)
    {
        var endpoint = configured ?? new EndpointSettings();
        string? Read(string name, string? fallback) => Value(options.Get($"{side}-{name}"),
            Environment.GetEnvironmentVariable($"S3M_{side.ToUpperInvariant()}_{name.ToUpperInvariant().Replace('-', '_')}"), fallback);
        endpoint.Url = EndpointSettings.NormalizeUrl(Required(Read("url", endpoint.Url), $"{side} S3 API URL"));
        endpoint.ClientId = Required(Read("client-id", endpoint.ClientId), $"{side} access key ID (client ID)");
        endpoint.Secret = Required(Read("secret", endpoint.Secret), $"{side} secret access key", secret: true);
        endpoint.Region = Value(Read("region", endpoint.Region)) ?? "us-east-1";
        endpoint.SessionToken = Read("session-token", endpoint.SessionToken);
        return endpoint;
    }

    private static string? Value(params string?[] values) => values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));

    private static string Required(string? value, string label, bool secret = false)
    {
        if (!string.IsNullOrWhiteSpace(value)) return value;
        if (Console.IsInputRedirected)
            throw new ArgumentException($"Missing {label}. Supply it through configuration, arguments, or environment variables.");
        Console.Write($"{label}: ");
        if (!secret)
            return Value(Console.ReadLine()?.Trim()) ?? throw new ArgumentException($"{label} is required.");

        var buffer = new StringBuilder();
        while (true)
        {
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter) break;
            if (key.Key == ConsoleKey.Backspace)
            {
                if (buffer.Length > 0) { buffer.Length--; Console.Write("\b \b"); }
            }
            else if (!char.IsControl(key.KeyChar))
            {
                buffer.Append(key.KeyChar);
                Console.Write('*');
            }
        }
        Console.WriteLine();
        return Value(buffer.ToString()) ?? throw new ArgumentException($"{label} is required.");
    }
}

internal sealed class EndpointSettings
{
    public string Url { get; set; } = "";
    public string ClientId { get; set; } = "";
    public string Secret { get; set; } = "";
    public string Region { get; set; } = "us-east-1";
    public string? SessionToken { get; set; }
    public bool ForcePathStyle { get; set; } = true;

    public static string NormalizeUrl(string value)
    {
        if (!Uri.TryCreate(value.Trim(), UriKind.Absolute, out var uri) ||
            uri.Scheme is not ("http" or "https") || string.IsNullOrEmpty(uri.Host) ||
            uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0 || uri.AbsolutePath != "/")
            throw new ArgumentException("Use an http(s) S3 API URL with no bucket, path, credentials, query, or fragment.");
        return uri.GetLeftPart(UriPartial.Authority);
    }

    public AmazonS3Client CreateClient()
    {
        AWSCredentials credentials = string.IsNullOrWhiteSpace(SessionToken)
            ? new BasicAWSCredentials(ClientId, Secret)
            : new SessionAWSCredentials(ClientId, Secret, SessionToken);
        return new AmazonS3Client(credentials, new AmazonS3Config
        {
            ServiceURL = Url,
            AuthenticationRegion = Region,
            ForcePathStyle = ForcePathStyle,
            RetryMode = RequestRetryMode.Standard,
            MaxErrorRetry = 4,
            // Avoid optional checksum trailers that older S3-compatible servers may reject.
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED,
            Timeout = TimeSpan.FromHours(24)
        });
    }
}
