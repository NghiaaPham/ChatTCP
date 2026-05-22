# Async TCP File Transfer Design

## Summary

Add asynchronous file transfer to the existing WPF TCP chat app without blocking normal chat. The app must support demo transfers up to 1 GB while users can continue sending and receiving text messages.

The design separates the lightweight chat/control channel from the heavy file byte stream:

- TCP port `5000`: existing chat/control protocol.
- TCP port `5001`: new file transfer protocol.

File bytes are streamed in chunks directly between network streams and file streams. The app must not load whole files into memory and must not encode file contents as JSON/base64.

## Goals

- Support sending files up to 1 GB in a classroom LAN demo.
- Keep text chat responsive during upload/download.
- Show upload and download progress in the WPF client.
- Let recipients download files on demand instead of auto-downloading.
- Keep the existing chat protocol mostly stable.
- Keep implementation scoped enough for a student demo but architected clearly.

## Non-Goals

- No resume after app restart in the first implementation pass.
- No peer-to-peer file transfer between WPF clients.
- No cloud storage or external HTTP server.
- No encryption beyond the existing LAN TCP transport.
- No virus scanning or enterprise file policy.

## Architecture

The system uses two logical planes.

### Control Plane: Chat TCP Port 5000

The existing server continues to accept chat clients on port `5000`. This port carries only small JSON packets:

- login and user list updates
- room joins
- text messages
- heartbeat
- file metadata notifications
- file transfer status events

This port must never carry raw file bytes.

### Data Plane: File TCP Port 5001

A new file transfer server listens on port `5001`. Each upload or download opens a separate TCP connection. This connection is short-lived and dedicated to one operation:

- one upload, or
- one download.

File bytes are streamed through a fixed-size buffer, recommended `256 KB`. This prevents large memory spikes while still providing decent LAN throughput.

## File Sending Flow

1. User clicks Attach in the WPF client.
2. Client opens a file picker.
3. Client validates file size is not larger than 1 GB.
4. Client creates a `TransferId`.
5. Client sends a small control packet on port `5000`:

   ```text
   FileOffer
   ```

   Fields:

   - `TransferId`
   - `RoomId`
   - `Sender`
   - `FileName`
   - `FileSize`
   - `CreatedAt`

6. Server registers the file metadata as pending.
7. Server broadcasts a file message into the room so other users see that a file is being sent.
8. Sender opens a new TCP connection to port `5001`.
9. Sender sends an upload header as one JSON line.
10. Sender streams raw file bytes in chunks.
11. Server writes bytes to `ServerFiles/{transferId}.part`.
12. When all bytes are received, server renames the file into a completed location:

    ```text
    ServerFiles/{transferId}/{safeFileName}
    ```

13. Server broadcasts `FileAvailable` on port `5000`.

## File Download Flow

1. Recipient sees a file card in the chat message list.
2. Recipient clicks Download.
3. Client opens a new TCP connection to port `5001`.
4. Client sends a download header as one JSON line.
5. Server validates the transfer exists and is complete.
6. Server streams raw file bytes to the client.
7. Client writes to a local download path using `.part`.
8. When complete, client renames `.part` to the final file name.
9. UI marks the transfer as downloaded.

## Protocol Additions

### Chat Packet Types on Port 5000

Add these packet types to both server and client packet enums:

- `FileOffer`
- `FileAvailable`
- `FileTransferFailed`

`FileOfferData`:

```csharp
public class FileOfferData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string CreatedAt { get; set; } = string.Empty;
}
```

`FileAvailableData`:

```csharp
public class FileAvailableData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
}
```

`FileTransferFailedData`:

```csharp
public class FileTransferFailedData
{
    public string TransferId { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}
```

### File Port Header Types on Port 5001

Each file connection starts with one JSON line header. After that header, the connection switches to raw bytes.

`UploadRequest`:

```json
{
  "type": "upload",
  "transferId": "...",
  "sender": "NghiaHost",
  "roomId": "General",
  "fileName": "demo.zip",
  "fileSize": 1073741824
}
```

`DownloadRequest`:

```json
{
  "type": "download",
  "transferId": "...",
  "requester": "NamAnh"
}
```

The file server responds with a small JSON line before streaming bytes:

```json
{
  "ok": true,
  "fileName": "demo.zip",
  "fileSize": 1073741824
}
```

If validation fails:

```json
{
  "ok": false,
  "error": "File is not available yet."
}
```

## Server Components

### `FileTransferServer`

Listens on TCP port `5001` and accepts file connections. It should run independently from the existing chat accept loop.

Responsibilities:

- accept file upload/download sessions
- parse one JSON header per file connection
- dispatch to upload or download handler
- keep file transfer failures isolated from chat sessions

### `FileTransferSession`

Handles one upload or download.

Responsibilities:

- stream bytes using `ReadAsync` and `WriteAsync`
- report progress internally
- write incomplete downloads/uploads with `.part`
- cleanup partial files on failure or cancel

### `FileMetadataStore`

Tracks file metadata in memory for the demo.

Responsibilities:

- register pending `FileOffer`
- mark file as available after successful upload
- resolve download requests by `TransferId`
- expose metadata to the chat router

For the first pass, metadata can be in memory. The actual file bytes are stored on disk.

## Client Components

### `IFileTransferService`

Public API for the ViewModel:

- `UploadFileAsync(...)`
- `DownloadFileAsync(...)`
- `CancelTransfer(...)`

It should expose progress events or observable transfer models.

### `FileTransferService`

Uses port `5001` to upload/download files. It must not use the chat `NetworkStream`.

Responsibilities:

- validate file size
- connect to file port
- send upload/download header
- stream bytes with a fixed buffer
- calculate progress percentage
- support cancellation

### `FileTransferItem`

UI model for each file transfer:

- `TransferId`
- `FileName`
- `FileSize`
- `Sender`
- `RoomId`
- `Status`
- `Progress`
- `LocalPath`
- `CanDownload`
- `CanCancel`

### `ChatViewModel`

Owns UI state and commands:

- `AttachFileCommand`
- `DownloadFileCommand`
- `CancelFileTransferCommand`

It should create file message cards when `FileOffer` or `FileAvailable` packets arrive.

## UI Design

Add a small Attach button near the composer. It should open a file picker.

File messages render as compact cards in the existing message list:

- file icon
- file name
- file size
- sender
- status text
- progress bar when uploading/downloading
- Download button when available
- Cancel button for active own transfers

The text composer remains enabled while file transfers are running.

## Error Handling

Upload failures:

- stop the file connection
- delete `.part`
- broadcast `FileTransferFailed`
- show failed state in sender UI

Download failures:

- stop the file connection
- delete local `.part`
- show failed state only to the downloading client

Server validation failures:

- reject files over 1 GB
- sanitize file names
- reject unknown transfer IDs
- reject download before upload completion

Disconnect behavior:

- chat disconnect should not corrupt already completed files
- file transfer disconnect should not kill chat
- active file transfers should be cancellable

## Performance Rules

- Buffer size: `256 KB` initially.
- No `File.ReadAllBytes`.
- No base64 file content.
- Use `FileStream` with async enabled.
- Write to `.part` then rename on success.
- Keep one transfer connection per upload/download.

## Test Plan

Build:

- `dotnet build ChatServer\ChatServer.csproj`
- `dotnet build WpfChatClient\WpfChatClient.csproj`

Functional:

- Send normal chat while a file upload is active.
- Send normal chat while a file download is active.
- Upload a small file and download it successfully.
- Upload a file close to 1 GB and confirm memory stays stable.
- Cancel an upload and confirm `.part` is removed.
- Cancel a download and confirm local `.part` is removed.
- Disconnect a client during upload and confirm other users can still chat.
- Try a duplicate file name and confirm server stores safely by `TransferId`.

Demo:

- Host runs server on the LAN.
- Two WPF clients connect.
- Client A uploads a large file.
- Client B keeps chatting during upload.
- Client B downloads after `FileAvailable`.

## Open Implementation Notes

- Use `ServerFiles` under the server app base directory for demo storage.
- Consider adding a simple cleanup at server startup to remove stale `.part` files.
- Keep metadata in memory for this pass; persistent metadata can be added later.
