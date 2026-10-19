namespace S3MigratorFromTemu;

internal sealed class CommandLine
{
    private static readonly HashSet<string> ValueOptions = new(StringComparer.Ordinal)
    {
        "config", "workdir", "directory", "bucket",
        "source-url", "source-client-id", "source-secret", "source-region", "source-session-token",
        "target-url", "target-client-id", "target-secret", "target-region", "target-session-token"
    };

    private readonly Dictionary<string, string> values = new(StringComparer.Ordinal);
    public string Command { get; private set; } = "";
    public bool Help { get; private set; }
    public bool Overwrite { get; private set; }
    public void EnableOverwrite() => Overwrite = true;
    public string? Get(string name) => values.GetValueOrDefault(name);

    public static CommandLine Parse(string[] args)
    {
        var result = new CommandLine();
        var index = 0;
        if (args.Length > 0 && !args[0].StartsWith('-'))
        {
            result.Command = args[0].ToLowerInvariant();
            index++;
        }
        if (result.Command is not ("" or "migrate" or "download" or "upload"))
            throw new ArgumentException("Command must be migrate, download, or upload. Use --help.");

        for (; index < args.Length; index++)
        {
            var argument = args[index];
            if (argument is "--help" or "-h") { result.Help = true; continue; }
            if (argument == "--overwrite") { result.Overwrite = true; continue; }
            var name = argument.StartsWith("--", StringComparison.Ordinal) ? argument[2..] : "";
            if (!ValueOptions.Contains(name))
                throw new ArgumentException($"Unknown option {argument}. Use --help.");
            if (++index >= args.Length || args[index].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Missing value for {argument}.");
            if (!result.values.TryAdd(name, args[index]))
                throw new ArgumentException($"Option {argument} was supplied more than once.");
        }
        return result;
    }

    public void SelectCommand(CancellationToken token)
    {
        if (Command.Length != 0) return;
        Command = Input.Interactive
            ? Input.Choice("What would you like to do?", token,
                ("download", "Save an S3 bucket to the filesystem only"),
                ("upload", "Upload from a local directory"),
                ("migrate", "Download a bucket and upload it now"))
            : "download";
    }

    public const string HelpText = """
        s3 migrator from temu

        Usage:
          dotnet run --project src/S3MigratorFromTemu -- [command] [options]

        Commands:
          download    Save to the filesystem only; no target details needed.
          upload      Upload a snapshot or ordinary local folder; no source details needed.
          migrate     Download the entire bucket, then upload it immediately.
          No command  Show the action menu (default choice: download only).

        Options:
          --config <file>                  Optional JSON settings file.
          --workdir <directory>            Local staging directory (default ./s3-migration).
          --directory <directory>          Folder to upload; snapshot roots auto-detected.
          --bucket <name>                  Source bucket; target bucket for ordinary folders.
          --source-url <url>               S3 API endpoint, e.g. http://localhost:9000.
          --source-client-id <access-key>  MinIO access key ID (not an OAuth client ID).
          --source-secret <secret>         Prefer an environment variable or masked prompt.
          --source-region <region>         Default us-east-1.
          --source-session-token <token>   Optional temporary-credential session token.
          --target-url <url>               Destination S3 API endpoint.
          --target-client-id <access-key>  Destination access key ID.
          --target-secret <secret>         Prefer an environment variable or masked prompt.
          --target-region <region>         Default us-east-1.
          --target-session-token <token>   Optional temporary-credential session token.
          --overwrite                     Allow replacing different existing target objects.
          --help, -h                      Show this help.

        Credentials can come from S3M_SOURCE_CLIENT_ID / S3M_SOURCE_SECRET and
        S3M_TARGET_CLIENT_ID / S3M_TARGET_SECRET. Missing required inputs are prompted.
        See README.md for all variables and configuration details.
        """;
}
