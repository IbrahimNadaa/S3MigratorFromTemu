using System.Text;
using System.Text.RegularExpressions;

namespace S3MigratorFromTemu;

internal static class Input
{
    public static bool Interactive => !Console.IsInputRedirected;

    public static string Read(string label, CancellationToken token, string? initial = null,
        string? defaultValue = null, Func<string, string>? normalize = null, bool secret = false, bool allowEmpty = false)
    {
        var candidate = initial;
        while (true)
        {
            token.ThrowIfCancellationRequested();
            if (candidate is null)
            {
                if (!Interactive)
                    candidate = defaultValue ?? throw new ArgumentException($"Missing {label}. Supply it through arguments, environment variables, or configuration.");
                else
                {
                    var hint = defaultValue is null ? "" : secret ? " [Enter to keep current value]" : $" [{defaultValue}]";
                    Console.Write($"{label}{hint}: ");
                    candidate = ReadLine(secret, token);
                    if (candidate.Length == 0 && defaultValue is not null) candidate = defaultValue;
                }
            }
            try
            {
                if (!allowEmpty && string.IsNullOrWhiteSpace(candidate))
                    throw new ArgumentException($"{label} is required.");
                if (!secret) candidate = candidate.Trim();
                return normalize is null ? candidate : normalize(candidate);
            }
            catch (Exception exception) when (IsInputError(exception))
            {
                if (!Interactive) throw new ArgumentException($"Invalid {label}: {exception.Message}");
                Error($"Invalid {label}: {exception.Message} Please try again.");
                candidate = null;
            }
        }
    }

    public static string Choice(string title, CancellationToken token, params (string Value, string Label)[] choices)
    {
        Console.WriteLine(title);
        for (var index = 0; index < choices.Length; index++) Console.WriteLine($"  {index + 1}. {choices[index].Label}");
        return Read("Choose an option", token, defaultValue: "1", normalize: value =>
        {
            if (int.TryParse(value, out var index) && index >= 1 && index <= choices.Length) return choices[index - 1].Value;
            foreach (var choice in choices)
                if (string.Equals(value, choice.Value, StringComparison.OrdinalIgnoreCase)) return choice.Value;
            throw new ArgumentException($"Enter a number from 1 to {choices.Length}.");
        });
    }

    public static bool IsInputError(Exception exception) =>
        exception is ArgumentException or IOException or UnauthorizedAccessException or NotSupportedException;

    public static void Error(string message) => Console.Error.WriteLine(message);

    public static string Bucket(string value)
    {
        if (!Regex.IsMatch(value, "^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", RegexOptions.CultureInvariant) ||
            value.Contains("..", StringComparison.Ordinal) || System.Net.IPAddress.TryParse(value, out _))
            throw new ArgumentException("Use 3-63 lowercase letters, numbers, dots, or hyphens; start/end with a letter or number. Consecutive dots and IP addresses are not bucket names.");
        return value;
    }

    public static string Region(string value)
    {
        if (!Regex.IsMatch(value, "^[a-z0-9][a-z0-9-]*$", RegexOptions.CultureInvariant))
            throw new ArgumentException("Use a signing region such as us-east-1, without spaces.");
        return value;
    }

    public static string Credential(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Any(char.IsControl))
            throw new ArgumentException("Enter a nonempty value without control characters.");
        return value;
    }

    public static string DirectoryPath(string value, bool mustExist)
    {
        var path = Path.GetFullPath(value.Trim().Trim('"'));
        if (File.Exists(path)) throw new ArgumentException("This path points to a file. Choose a directory.");
        if (mustExist && !Directory.Exists(path)) throw new DirectoryNotFoundException("The directory does not exist.");
        if (Directory.Exists(path) && (File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0)
            throw new ArgumentException("Choose a directory without a symbolic link or junction.");
        return path;
    }

    private static string ReadLine(bool secret, CancellationToken token)
    {
        var buffer = new StringBuilder();
        // Poll before ReadKey so Ctrl+C also cancels a prompt waiting for input.
        while (true)
        {
            token.ThrowIfCancellationRequested();
            if (!Console.KeyAvailable) { Thread.Sleep(25); continue; }
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter) { Console.WriteLine(); return buffer.ToString(); }
            if (key.Key == ConsoleKey.Backspace)
            {
                if (buffer.Length > 0) { buffer.Length--; Console.Write("\b \b"); }
            }
            else if (!char.IsControl(key.KeyChar))
            {
                buffer.Append(key.KeyChar);
                Console.Write(secret ? '*' : key.KeyChar);
            }
        }
    }
}
