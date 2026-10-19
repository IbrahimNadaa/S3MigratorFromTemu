# s3 migrator from temu

A C# / .NET 10 console application that downloads every current object in a MinIO/S3 bucket to the local filesystem, then uploads that completed snapshot to another S3 server using **the same bucket name and exact object keys**.

The source project is supplied without running a build or adding tests, as requested. No migration has been run against a real server.

## Quick start

Requires the .NET 10 SDK and enough disk space for the full bucket. From this project directory, run:

```powershell
dotnet run --project ./src/S3MigratorFromTemu -- migrate
```

Enter the source URL, bucket name, source access key ID and secret, and destination connection details when prompted. Secrets are masked. In MinIO, the requested **client ID means the S3 access key ID**, such as a service account's access key; it is not an OAuth/OIDC client ID.

Use the S3 API endpoint (often port `9000` for MinIO), for example `http://localhost:9000`. Supply the bucket separately. A MinIO console URL (often port `9001`) is not an S3 API URL.

If the destination bucket is missing, the app creates it. If it already exists, the app uses it. An object with matching bytes and preserved metadata is skipped after SHA-256 verification. A different existing object stops the upload unless you pass `--overwrite`. Additional destination objects are retained.

## Configure endpoints and credentials

Copy `migration.example.json` to `migration.json` and set the bucket and both API URLs. Leave credentials blank in the file and use environment variables or masked prompts:

```powershell
$env:S3M_SOURCE_CLIENT_ID = 'source-access-key'
$env:S3M_TARGET_CLIENT_ID = 'target-access-key'

# Set secrets from your existing secret manager/environment, or leave them
# unset and the app will prompt with masked input.
dotnet run --project ./src/S3MigratorFromTemu -- migrate --config ./migration.json
```

You can set `S3M_SOURCE_SECRET` and `S3M_TARGET_SECRET` for unattended runs. Credentials are never written into migration state. Configuration supports `clientId`, `secret`, and optional `sessionToken` if you choose to supply them there. Keep a credential-bearing configuration file private; `migration.json` is gitignored.

Settings use this precedence: **command-line value > environment variable > configuration file > prompt/default**. Relative paths are resolved against the current working directory.

| Setting | Environment variable | Default |
| --- | --- | --- |
| Bucket | `S3M_BUCKET` | Prompt during download/migrate; read from snapshot during upload |
| Local directory | `S3M_WORKDIR` | `./s3-migration` |
| Source S3 API URL | `S3M_SOURCE_URL` | Prompt |
| Source access key ID | `S3M_SOURCE_CLIENT_ID` | Prompt |
| Source secret key | `S3M_SOURCE_SECRET` | Masked prompt |
| Source signing region | `S3M_SOURCE_REGION` | `us-east-1` |
| Source session token | `S3M_SOURCE_SESSION_TOKEN` | None |
| Target S3 API URL | `S3M_TARGET_URL` | Prompt |
| Target access key ID | `S3M_TARGET_CLIENT_ID` | Prompt |
| Target secret key | `S3M_TARGET_SECRET` | Masked prompt |
| Target signing region | `S3M_TARGET_REGION` | `us-east-1` |
| Target session token | `S3M_TARGET_SESSION_TOKEN` | None |

The JSON `forcePathStyle` setting defaults to `true` independently for both endpoints, which suits MinIO. Set it to `false` if your provider requires virtual-hosted addressing. Use the actual region of each server. TLS certificates use normal system validation.

For piped/noninteractive input, required settings must be supplied through arguments, environment variables, or configuration; the app does not read secrets from standard input.

## Download now, upload later

```powershell
# Needs only the source connection. Downloads the entire bucket to disk.
dotnet run --project ./src/S3MigratorFromTemu -- download --config ./migration.json

# Needs only the target connection and the completed local snapshot.
dotnet run --project ./src/S3MigratorFromTemu -- upload --config ./migration.json

# Explicitly allow replacing different objects already at the destination.
dotnet run --project ./src/S3MigratorFromTemu -- upload --config ./migration.json --overwrite
```

For a command-line-only configuration:

```powershell
dotnet run --project ./src/S3MigratorFromTemu -- migrate `
  --source-url 'https://source.example.com' `
  --target-url 'https://target.example.com' `
  --bucket 'my-bucket' `
  --source-client-id 'source-access-key' `
  --target-client-id 'target-access-key' `
  --workdir 'D:\s3-migration'
```

The app prompts for both secrets. `--source-secret` and `--target-secret` also exist for integrations, but their values can appear in shell history and process arguments. Prefer environment variables or the prompts.

Run with `--help` for every command-line option. The default command is `migrate`.

## Local files and exact object names

```text
s3-migration/
  workspace.json             Source identity, bucket, completion flag, manifest hash/totals
  manifest.jsonl             Completed snapshot: one object record per line
  manifest.pending.jsonl     Present while a download is incomplete
  files/
    images/logo.png          Ordinary keys retain their relative names and directories
    documents/report.pdf
  special/<prefix>/<hash>.bin
                             Content of keys that cannot be mirrored safely
  cache/<prefix>/<hash>.json  Per-object download checkpoints
  partials/<hash>.part        Downloads in progress
  .migration.lock            Prevents simultaneous runs against this local directory
```

Each work directory holds one source server/bucket/region. Use a new empty directory for a different source. Keep the entire directory together when moving the downloaded snapshot to another machine.

Only keys in the latest completed manifest are uploaded. Files from earlier downloads can remain on disk if those keys have since been removed from the source.

S3 allows names that local filesystems cannot represent. Windows reserved names, long paths, case collisions, file/directory collisions, empty path segments, backslashes, traversal-like segments, and trailing `/` folder marker objects use `special/`. Their original keys are recorded verbatim in the manifest. On upload, the app always uses that original key, preserving the destination hierarchy and names. Zero-byte objects and the actual contents of folder marker objects are included.

Local paths are confined to the staging directory. The app rejects staging symlinks and junctions. Use the staging directory only for this migration and keep files in place until uploading has finished.

## Verification and interruption

- Source listings are paginated, so buckets larger than 1,000 objects are included.
- Downloads stream to temporary files, calculate SHA-256, check the advertised length, then move the completed content into place.
- Each downloaded object gets an atomic checkpoint. On another download, a matching source ETag, size, modification time, and local SHA-256 allow that object to be reused.
- Uploading is allowed only after a complete source listing and successful downloads. A failed download marks the snapshot incomplete, even if an older completed manifest exists.
- The completed manifest's SHA-256, object count, and byte count are checked before any destination writes, detecting a changed or truncated manifest.
- Every local object is checked against its recorded SHA-256 before upload. Large uploads use the SDK's multipart transfer support with a part size selected to stay within the part-count limit.
- Matching destination objects are read and hashed before being skipped. Each newly uploaded object is read back and checked for size, SHA-256, and preserved metadata. **Budget for the destination read traffic and `GetObject` permission.**
- Without `--overwrite`, conditional writes also protect against a destination object appearing between the comparison and upload. The destination must support S3 conditional writes, including multipart completion. A server that rejects them needs compatible server software or an intentional `--overwrite` run.
- Requests use SDK retries. Interrupted download streams get up to three whole-object attempts. Retry by rerunning the same command; a partially transferred individual object starts again, while completed matching objects can be reused.
- Press Ctrl+C to cancel. Exit codes: `0` success, `1` failure, `130` cancellation. Local data is retained. An interrupted multipart upload can leave unfinished parts on the server; the destination's multipart lifecycle cleanup can remove them.

The app stops on the first unrecovered error and does not report a partial migration as successful. Upload retries may skip objects successfully transferred by an earlier run after verifying their content and metadata.

## Scope and permissions

This migrates **current visible object contents**, their keys, user-defined metadata, and these HTTP properties: content type, cache control, content disposition, content encoding, content language, and expiry. It does not copy historical versions, delete markers, object tags, ACLs, bucket policies, lifecycle rules, notifications, storage-class settings, object retention/legal holds, encryption configuration, or original S3 modification timestamps. Configure any required bucket settings on the destination separately. Destination encryption follows the destination's defaults.

Source changes during pagination do not produce an atomic bucket snapshot. Pause source writes for a consistent migration. Conditional GETs detect content changing between listing and download; objects must also be readable through S3 (restore archived objects first if required).

The source needs `s3:ListBucket` and `s3:GetObject`. The target needs `s3:GetBucketLocation`, `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, and `s3:AbortMultipartUpload`; also `s3:CreateBucket` when the bucket is missing. Encryption keys or organization policies may require additional provider-specific access. No object deletion permission is needed.

The same bucket name must be available on the destination. A globally named Amazon S3 bucket that belongs to another account cannot be reused by this app. Supply two distinct server endpoints; the app rejects the exact same source and target API URL.

## SDK references

The implementation uses the [AWS SDK for .NET v4 S3 package](https://www.nuget.org/packages/AWSSDK.S3/4.0.104.3), [path-style endpoint configuration](https://docs.aws.amazon.com/sdkfornet/v4/apidocs/items/S3/TS3Config.html), and [TransferUtility multipart uploads](https://docs.aws.amazon.com/sdkfornet/v4/apidocs/items/S3/TTransferUtility.html).
