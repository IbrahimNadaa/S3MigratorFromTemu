using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.Runtime;
using Amazon.S3;

namespace S3MigratorFromTemu;

internal sealed class Settings
{
    public string? Bucket { get; set; }
    public string WorkDirectory { get; set; } = "./s3-migration";
    public string? UploadDirectory { get; set; }
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
        var settings = new Settings();
        while (configPath is not null)
        {
            try
            {
                settings = JsonSerializer.Deserialize<Settings>(await File.ReadAllTextAsync(configPath, token), Json)
                    ?? throw new ArgumentException("The configuration file is empty.");
                break;
            }
            catch (Exception exception) when (Input.IsInputError(exception) || exception is JsonException)
            {
                if (!Input.Interactive) throw;
                Input.Error("Cannot read the configuration file. Check its path and JSON property names/values.");
                var next = Input.Read("Configuration file (Enter to use prompts instead)", token, allowEmpty: true);
                configPath = next.Length == 0 ? null : next;
            }
        }

        settings.WorkDirectory = First(options.Get("workdir"), Environment.GetEnvironmentVariable("S3M_WORKDIR"),
            settings.WorkDirectory) ?? "./s3-migration";
        settings.UploadDirectory = First(options.Get("directory"), options.Get("workdir"),
            Environment.GetEnvironmentVariable("S3M_DIRECTORY"), Environment.GetEnvironmentVariable("S3M_WORKDIR"),
            settings.UploadDirectory);
        settings.Bucket = First(options.Get("bucket"), Environment.GetEnvironmentVariable("S3M_BUCKET"), settings.Bucket);
        if (options.Command != "upload")
        {
            settings.Bucket = Input.Read("Source bucket name", token, initial: settings.Bucket, normalize: Input.Bucket);
            settings.Source = ResolveEndpoint(options, "source", settings.Source, token);
            settings.WorkDirectory = Input.Read("Directory to save the files", token,
                initial: First(options.Get("workdir"), Environment.GetEnvironmentVariable("S3M_WORKDIR")),
                defaultValue: settings.WorkDirectory, normalize: value => Input.DirectoryPath(value, mustExist: false));
        }
        else
        {
            settings.Source = null;
            settings.UploadDirectory = Input.Read("Directory to upload (snapshot folder or ordinary folder)", token,
                initial: settings.UploadDirectory,
                defaultValue: settings.WorkDirectory, normalize: value => Input.DirectoryPath(value, mustExist: true));
        }
        return settings;
    }

    public void ResolveTarget(CommandLine options, string? sourceUrl, CancellationToken token) =>
        Target = ResolveEndpoint(options, "target", Target, token, sourceUrl);

    private static EndpointSettings ResolveEndpoint(CommandLine options, string side, EndpointSettings? configured,
        CancellationToken token, string? forbiddenUrl = null)
    {
        var endpoint = configured ?? new EndpointSettings();
        string? Read(string name, string? fallback) => First(options.Get($"{side}-{name}"),
            Environment.GetEnvironmentVariable($"S3M_{side.ToUpperInvariant()}_{name.ToUpperInvariant().Replace('-', '_')}"), fallback);
        endpoint.Url = Input.Read($"{side} S3 API URL", token, initial: Read("url", endpoint.Url),
            normalize: value => EndpointSettings.ValidateUrl(value, forbiddenUrl));
        endpoint.ClientId = Input.Read($"{side} access key ID (client ID)", token,
            initial: Read("client-id", endpoint.ClientId), normalize: Input.Credential);
        endpoint.Secret = Input.Read($"{side} secret access key", token, initial: Read("secret", endpoint.Secret),
            normalize: Input.Credential, secret: true);
        endpoint.Region = Input.Read($"{side} signing region", token, initial: Read("region", endpoint.Region),
            defaultValue: "us-east-1", normalize: Input.Region);
        endpoint.SessionToken = Read("session-token", endpoint.SessionToken);
        return endpoint;
    }

    public void CorrectConnection(EndpointSettings endpoint, string side, bool allowBucketChange,
        string? forbiddenUrl, CancellationToken token)
    {
        var choices = new List<(string Value, string Label)>
        {
            ("url", $"Change {side} S3 API URL"),
            ("client", $"Change {side} access key ID"),
            ("secret", $"Re-enter {side} secret access key"),
            ("region", $"Change signing region (currently {endpoint.Region})"),
            ("token", "Change/clear temporary session token"),
            ("style", "Change path-style addressing"),
            ("retry", "Retry with the same settings")
        };
        if (allowBucketChange) choices.Add(("bucket", "Change bucket name"));
        choices.Add(("cancel", "Cancel"));
        var change = Input.Choice("Correct the connection details:", token, choices.ToArray());
        switch (change)
        {
            case "url":
                endpoint.Url = Input.Read($"{side} S3 API URL", token, defaultValue: endpoint.Url,
                    normalize: value => EndpointSettings.ValidateUrl(value, forbiddenUrl));
                break;
            case "client":
                endpoint.ClientId = Input.Read($"{side} access key ID", token, defaultValue: endpoint.ClientId,
                    normalize: Input.Credential);
                break;
            case "secret":
                endpoint.Secret = Input.Read($"{side} secret access key", token, normalize: Input.Credential, secret: true);
                break;
            case "region":
                endpoint.Region = Input.Read($"{side} signing region", token, defaultValue: endpoint.Region, normalize: Input.Region);
                break;
            case "token":
                endpoint.SessionToken = First(Input.Read($"{side} session token (Enter to clear)", token, secret: true, allowEmpty: true));
                break;
            case "style":
                endpoint.ForcePathStyle = Input.Choice("Use path-style addressing?", token,
                    ("yes", "Yes (MinIO default)"), ("no", "No (virtual-hosted addressing)")) == "yes";
                break;
            case "bucket":
                Bucket = Input.Read("Bucket name", token, defaultValue: Bucket, normalize: Input.Bucket);
                break;
            case "cancel":
                throw new OperationCanceledException(new CancellationToken(canceled: true));
        }
    }

    private static string? First(params string?[] values) => values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value));
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

    public static string ValidateUrl(string value, string? forbiddenUrl)
    {
        var normalized = NormalizeUrl(value);
        if (string.Equals(normalized, forbiddenUrl, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Choose a target S3 server different from the source server.");
        return normalized;
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
            RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
            ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED,
            Timeout = TimeSpan.FromHours(24)
        });
    }
}
