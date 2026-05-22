# Async TCP File Transfer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add asynchronous TCP file upload and download up to 1 GB while normal chat stays responsive.

**Architecture:** Keep the existing chat/control JSON line protocol on TCP port `5000`. Add a dedicated file-transfer TCP listener on port `5001`; each upload or download opens one short-lived data connection and streams bytes through async `NetworkStream` and `FileStream` calls. The WPF client updates file-card progress from a separate file-transfer service so the message textbox and chat receive loop remain usable during large transfers.

**Tech Stack:** .NET 9, WPF, CommunityToolkit.Mvvm, `System.Net.Sockets`, `System.Text.Json`, async `FileStream`, `Microsoft.Win32.OpenFileDialog`, `Microsoft.Win32.SaveFileDialog`.

---

## Constants And Rules

- Chat port remains `5000`.
- File data port is `5001`.
- Maximum file size is `1_073_741_824` bytes.
- Stream buffer size is `262_144` bytes.
- Server storage root is `Path.Combine(AppContext.BaseDirectory, "ServerFiles")`.
- A transfer writes to `.part` first and moves to the final path only after the expected byte count is received.
- File data port must use a manual JSON-line reader that stops exactly at `\n`. Do not use `StreamReader` for file-port headers because it can buffer raw file bytes after the header.
- Chat port carries only metadata packets: `FileOffer`, `FileAvailable`, and `FileTransferFailed`.

## Task 1: Add Shared Chat Packet Types

Files:

- `C:\Users\ADMIN\Documents\Chat\ChatServer\Program.cs`
- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Core\Models\Packets.cs`

Steps:

- [ ] In both `PacketType` enums, add these values after `ConnectionRejected`:

```csharp
FileOffer,
FileAvailable,
FileTransferFailed
```

- [ ] In both files, add these data classes after `HeartbeatData`:

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

public class FileAvailableData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
}

public class FileTransferFailedData
{
    public string TransferId { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}
```

- [ ] Build both projects:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanCheck1
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck1
```

Expected output: both commands end with `Build succeeded`.

## Task 2: Add Server File Transfer Metadata Store

Files to create:

- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileTransferStatus.cs`
- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileTransferRecord.cs`
- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileMetadataStore.cs`

Steps:

- [ ] Create `FileTransferStatus.cs`:

```csharp
namespace ChatServer.FileTransfers;

public enum FileTransferStatus
{
    Pending,
    Available,
    Failed
}
```

- [ ] Create `FileTransferRecord.cs`:

```csharp
namespace ChatServer.FileTransfers;

public sealed class FileTransferRecord
{
    public required string TransferId { get; init; }
    public required string RoomId { get; init; }
    public required string Sender { get; init; }
    public required string OriginalFileName { get; init; }
    public required string SafeFileName { get; init; }
    public required long FileSize { get; init; }
    public required DateTime CreatedAtUtc { get; init; }
    public FileTransferStatus Status { get; set; }
    public string? FinalPath { get; set; }
    public string? FailureReason { get; set; }
}
```

- [ ] Create `FileMetadataStore.cs` with these public members:

```csharp
namespace ChatServer.FileTransfers;

public sealed class FileMetadataStore
{
    public const long MaxFileSizeBytes = 1_073_741_824;
    public const int BufferSizeBytes = 262_144;

    public FileMetadataStore(string storageRoot);
    public string StorageRoot { get; }
    public bool TryRegisterOffer(FileOfferData offer, string authenticatedSender, out FileTransferRecord record, out string error);
    public bool TryGet(string transferId, out FileTransferRecord record);
    public bool TryMarkAvailable(string transferId, string finalPath, out FileTransferRecord record);
    public bool TryMarkFailed(string transferId, string reason, out FileTransferRecord record);
    public string GetPartialUploadPath(string transferId);
    public string GetCompletedDirectory(string transferId);
    public void CleanupStalePartialFiles();
}
```

- [ ] Implement the store with a `ConcurrentDictionary<string, FileTransferRecord>` keyed by `TransferId`.
- [ ] `TryRegisterOffer` must reject blank transfer IDs, blank file names, blank room IDs, mismatched sender values, and file sizes less than `1` or greater than `MaxFileSizeBytes`.
- [ ] `TryRegisterOffer` must sanitize file names with `Path.GetFileName`, replace invalid file-name characters with `_`, and store the authenticated server-side username instead of trusting the client `Sender`.
- [ ] `CleanupStalePartialFiles` must delete files matching `*.part` under `StorageRoot` at server startup.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanCheck2
```

Expected output: `Build succeeded`.

## Task 3: Add Manual File-Port Protocol Helpers On Server

Files to create:

- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileTransferProtocol.cs`

Steps:

- [ ] Create request/response models in this file:

```csharp
namespace ChatServer.FileTransfers;

public sealed class FilePortRequest
{
    public string Type { get; set; } = string.Empty;
    public string TransferId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string Requester { get; set; } = string.Empty;
}

public sealed class FilePortResponse
{
    public bool Ok { get; set; }
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string Error { get; set; } = string.Empty;
}
```

- [ ] Add a static `FileTransferProtocol` class with:

```csharp
public static readonly JsonSerializerOptions JsonOptions = new()
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    PropertyNameCaseInsensitive = true
};

public static async Task<string> ReadJsonLineAsync(NetworkStream stream, CancellationToken token)
public static async Task WriteJsonLineAsync(NetworkStream stream, object payload, CancellationToken token)
```

- [ ] `ReadJsonLineAsync` must read one byte at a time until `\n`, ignore `\r`, reject lines over `16_384` bytes, and decode UTF-8 after the newline is found.
- [ ] `WriteJsonLineAsync` must serialize with `JsonOptions`, append `\n`, write bytes to the stream, and flush.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanCheck3
```

Expected output: `Build succeeded`.

## Task 4: Add Server Upload And Download Sessions

Files to create:

- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileTransferSession.cs`
- `C:\Users\ADMIN\Documents\Chat\ChatServer\FileTransfers\FileTransferServer.cs`

Steps:

- [ ] Create `FileTransferSession` with constructor:

```csharp
public FileTransferSession(
    TcpClient client,
    FileMetadataStore metadataStore,
    Func<FileTransferRecord, Task> onAvailable,
    Func<FileTransferRecord, Task> onFailed)
```

- [ ] Add `RunAsync(CancellationToken token)` that reads a `FilePortRequest` from the network stream and dispatches on `Type` values `upload` and `download`.
- [ ] Upload behavior:
  - validate `TransferId` exists in `FileMetadataStore`;
  - validate request `FileSize` matches the registered offer;
  - send `{ ok: true, fileName, fileSize }` before reading bytes;
  - stream exactly `FileSize` bytes from `NetworkStream` into `GetPartialUploadPath(transferId)`;
  - use `new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, FileMetadataStore.BufferSizeBytes, useAsync: true)`;
  - move `.part` into `ServerFiles/{transferId}/{safeFileName}` after full length is received;
  - call `metadataStore.TryMarkAvailable` and then `onAvailable(record)`.
- [ ] Upload failure behavior:
  - delete the `.part` file for the transfer;
  - call `metadataStore.TryMarkFailed`;
  - call `onFailed(record)` when a record exists;
  - never throw out of `RunAsync` after cleanup.
- [ ] Download behavior:
  - validate `TransferId` exists and `Status == Available`;
  - validate `FinalPath` exists;
  - send `{ ok: true, fileName, fileSize }`;
  - stream file bytes to `NetworkStream` using the same buffer size.
- [ ] Download validation failures must send `{ ok: false, error }` and close only that file data connection.
- [ ] Create `FileTransferServer` with:

```csharp
public FileTransferServer(
    int port,
    FileMetadataStore metadataStore,
    Func<FileTransferRecord, Task> onAvailable,
    Func<FileTransferRecord, Task> onFailed)

public Task StartAsync(CancellationToken token)
```

- [ ] `StartAsync` must start `TcpListener` on `IPAddress.Any`, accept clients asynchronously, and start one `FileTransferSession.RunAsync` task per accepted TCP client.
- [ ] Log these server messages:
  - `[FILE] server listening on TCP port 5001`
  - `[FILE] upload started: {transferId} {fileName} {fileSize} bytes`
  - `[FILE] upload completed: {transferId}`
  - `[FILE] download started: {transferId} by {requester}`
  - `[FILE] transfer failed: {transferId} {reason}`

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanCheck4
```

Expected output: `Build succeeded`.

## Task 5: Wire File Transfer Into The Server Chat Router

Files:

- `C:\Users\ADMIN\Documents\Chat\ChatServer\Program.cs`

Steps:

- [ ] Add `using ChatServer.FileTransfers;` at the top.
- [ ] Change `MessageRouter` constructor to accept `FileMetadataStore metadataStore`.
- [ ] Add `PacketType.FileOffer` handling in `MessageRouter.RouteAsync`:
  - deserialize `FileOfferData`;
  - force `Sender = session.Username`;
  - call `metadataStore.TryRegisterOffer`;
  - if validation fails, send `FileTransferFailed` only to the sender session;
  - if validation succeeds, broadcast `FileOffer` to `RoomId` using `BroadcastToRoomAsync`, except use `BroadcastAsync` when `RoomId` is `General`.
- [ ] Add `BroadcastFileAvailableAsync(FileTransferRecord record)`:

```csharp
public Task BroadcastFileAvailableAsync(FileTransferRecord record)
```

This method must send `PacketType.FileAvailable` with `TransferId`, `RoomId`, `FileName`, and `FileSize`.

- [ ] Add `BroadcastFileFailedAsync(FileTransferRecord record)`:

```csharp
public Task BroadcastFileFailedAsync(FileTransferRecord record)
```

This method must send `PacketType.FileTransferFailed` with `TransferId` and `FailureReason`.

- [ ] Update `Program` static fields:

```csharp
private const int ChatPort = 5000;
private const int FilePort = 5001;
private static readonly ConnectedClients _clients = new();
private static readonly FileMetadataStore _fileStore = new(Path.Combine(AppContext.BaseDirectory, "ServerFiles"));
private static readonly MessageRouter _router = new(_clients, _fileStore);
private static readonly FileTransferServer _fileServer = new(
    FilePort,
    _fileStore,
    record => _router.BroadcastFileAvailableAsync(record),
    record => _router.BroadcastFileFailedAsync(record));
```

- [ ] In `Main`, call `_fileStore.CleanupStalePartialFiles();` before starting listeners.
- [ ] In `Main`, start the file server without blocking the chat accept loop:

```csharp
using var serverCts = new CancellationTokenSource();
var fileServerTask = _fileServer.StartAsync(serverCts.Token);
```

- [ ] Keep the existing chat listener loop on port `5000`.
- [ ] Update `PrintBanner` so it prints both ports:

```text
Server is listening on TCP port 5000 for chat
Server is listening on TCP port 5001 for files
```

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanCheck5
```

Expected output: `Build succeeded`.

## Task 6: Add Client File Transfer Models

Files to create:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Models\FileTransferUiStatus.cs`
- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Models\FileTransferItem.cs`

File to edit:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Models\ChatMessage.cs`

Steps:

- [ ] Create `FileTransferUiStatus.cs`:

```csharp
namespace WpfChatClient.Models;

public enum FileTransferUiStatus
{
    Pending,
    Uploading,
    Available,
    Downloading,
    Downloaded,
    Failed,
    Canceled
}
```

- [ ] Create `FileTransferItem.cs` as a `partial class` deriving from `ObservableObject` with observable properties:

```csharp
public partial class FileTransferItem : ObservableObject
{
    public const long MaxFileSizeBytes = 1_073_741_824;

    [ObservableProperty] private string _transferId = string.Empty;
    [ObservableProperty] private string _fileName = string.Empty;
    [ObservableProperty] private long _fileSize;
    [ObservableProperty] private string _sender = string.Empty;
    [ObservableProperty] private string _roomId = "General";
    [ObservableProperty] private FileTransferUiStatus _status = FileTransferUiStatus.Pending;
    [ObservableProperty] private double _progress;
    [ObservableProperty] private string _localPath = string.Empty;
    [ObservableProperty] private string _statusText = "Waiting";
    [ObservableProperty] private string _errorMessage = string.Empty;
    [ObservableProperty] private bool _isOwn;

    public string SizeText => FormatFileSize(FileSize);
    public bool IsBusy => Status is FileTransferUiStatus.Uploading or FileTransferUiStatus.Downloading;
    public bool CanDownload => !IsOwn && Status == FileTransferUiStatus.Available;
    public bool CanCancel => Status is FileTransferUiStatus.Uploading or FileTransferUiStatus.Downloading;
}
```

- [ ] Implement `partial void OnFileSizeChanged(long value)` and `partial void OnStatusChanged(FileTransferUiStatus value)` to raise `PropertyChanged` for `SizeText`, `IsBusy`, `CanDownload`, and `CanCancel`.
- [ ] Implement `FormatFileSize(long bytes)` with B, KB, MB, and GB display.
- [ ] Extend the `ChatMessage` record with:

```csharp
bool IsFile = false,
FileTransferItem? FileTransfer = null
```

- [ ] Change `IsText` to:

```csharp
public bool IsText => !IsSticker && !IsFile;
```

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck6
```

Expected output: `Build succeeded`.

## Task 7: Add Client File-Port Protocol And Service

Files to create:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Services\FileTransferProtocol.cs`
- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Core\Interfaces\IFileTransferService.cs`
- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Services\FileTransferService.cs`

Steps:

- [ ] Create `FileTransferProtocol.cs` in the client with the same `FilePortRequest`, `FilePortResponse`, `ReadJsonLineAsync`, and `WriteJsonLineAsync` behavior from Task 3.
- [ ] Create `IFileTransferService`:

```csharp
namespace WpfChatClient.Core.Interfaces;

public sealed record FileUploadRequest(
    string ServerIp,
    int FilePort,
    string TransferId,
    string Sender,
    string RoomId,
    string FilePath,
    string FileName,
    long FileSize);

public sealed record FileDownloadRequest(
    string ServerIp,
    int FilePort,
    string TransferId,
    string Requester,
    string DestinationPath);

public sealed record FileTransferProgress(long BytesTransferred, long TotalBytes, double Percent);

public interface IFileTransferService
{
    Task UploadFileAsync(FileUploadRequest request, IProgress<FileTransferProgress> progress, CancellationToken token);
    Task DownloadFileAsync(FileDownloadRequest request, IProgress<FileTransferProgress> progress, CancellationToken token);
}
```

- [ ] Implement `FileTransferService.UploadFileAsync`:
  - validate source file exists;
  - validate size is between `1` and `FileTransferItem.MaxFileSizeBytes`;
  - connect to `request.ServerIp:request.FilePort`;
  - write upload header with `type = "upload"`;
  - read `FilePortResponse`;
  - throw `InvalidOperationException(response.Error)` when `Ok == false`;
  - stream from source `FileStream` to `NetworkStream` with a `262_144` byte buffer;
  - report progress after every chunk.
- [ ] Implement `FileTransferService.DownloadFileAsync`:
  - connect to file port;
  - write download header with `type = "download"`;
  - read response;
  - write to `DestinationPath + ".part"`;
  - move `.part` to `DestinationPath` after full byte count is received;
  - delete `.part` on exception or cancellation.
- [ ] Ensure both methods use `ConfigureAwait(false)` inside service code and never access WPF UI types.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck7
```

Expected output: `Build succeeded`.

## Task 8: Expose File Control Packets Through ChatService

Files:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Core\Interfaces\IChatService.cs`
- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Services\ChatService.cs`

Steps:

- [ ] Add delegates to `IChatService.cs`:

```csharp
public delegate void FileOfferReceivedHandler(FileOfferData offer);
public delegate void FileAvailableReceivedHandler(FileAvailableData file);
public delegate void FileTransferFailedReceivedHandler(FileTransferFailedData failure);
```

- [ ] Add events to `IChatService`:

```csharp
event FileOfferReceivedHandler FileOfferReceived;
event FileAvailableReceivedHandler FileAvailableReceived;
event FileTransferFailedReceivedHandler FileTransferFailedReceived;
```

- [ ] Add connection target properties:

```csharp
string? ServerIp { get; }
int ServerPort { get; }
int FilePort { get; }
```

- [ ] Add method:

```csharp
Task<bool> SendFileOfferAsync(FileOfferData offer);
```

- [ ] Implement `ServerIp`, `ServerPort`, and `FilePort` in `ChatService`; `FilePort` must return `_lastPort + 1` when `_lastPort > 0`, otherwise `5001`.
- [ ] In `ConnectAsync`, set `ServerIp` and `ServerPort` from the normalized connection target.
- [ ] Implement `SendFileOfferAsync` by sending `PacketType.FileOffer` through `TrySendPacketAsync`.
- [ ] Extend `ParseLine`:
  - `PacketType.FileOffer`: deserialize `FileOfferData`, invoke `FileOfferReceived`.
  - `PacketType.FileAvailable`: deserialize `FileAvailableData`, invoke `FileAvailableReceived`.
  - `PacketType.FileTransferFailed`: deserialize `FileTransferFailedData`, invoke `FileTransferFailedReceived`.
- [ ] Do not save file packets into `MessageCache` in this pass.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck8
```

Expected output: `Build succeeded`.

## Task 9: Register The File Transfer Service In DI

File:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\App.xaml.cs`

Steps:

- [ ] Add this service registration below `IStickerService`:

```csharp
services.AddSingleton<IFileTransferService, FileTransferService>();
```

- [ ] Keep `ChatViewModel` as singleton so file transfer UI state survives view switches.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck9
```

Expected output: `Build succeeded`.

## Task 10: Add File Transfer State To ChatViewModel

File:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\ViewModels\ChatViewModel.cs`

Steps:

- [ ] Inject `IFileTransferService fileTransferService` into the constructor and store it in a readonly field.
- [ ] Add fields:

```csharp
private readonly IFileTransferService _fileTransferService;
private readonly Dictionary<string, FileTransferItem> _fileTransfers = new(StringComparer.OrdinalIgnoreCase);
private readonly Dictionary<string, CancellationTokenSource> _fileTransferCancellations = new(StringComparer.OrdinalIgnoreCase);
```

- [ ] Subscribe to `_chatService.FileOfferReceived`, `_chatService.FileAvailableReceived`, and `_chatService.FileTransferFailedReceived` in the constructor.
- [ ] Unsubscribe from these three events in `Dispose`.
- [ ] Add helper `CreateFileMessage(FileTransferItem transfer)` that returns a `ChatMessage` with `IsFile = true`, `FileTransfer = transfer`, `Content = transfer.FileName`, and avatar color from `transfer.Sender`.
- [ ] Add helper `AddOrUpdateFileOffer(FileOfferData offer)`:
  - create a `FileTransferItem` when the transfer ID is new;
  - set status to `Pending` for non-own transfers and `Uploading` for own transfers;
  - add one file message to the matching room using `TryAddMessageToRoom`;
  - add to `Messages` only when `offer.RoomId` equals `CurrentRoomId`.
- [ ] Add helper `MarkFileAvailable(FileAvailableData file)`:
  - find the transfer by ID;
  - set `Status = Available`, `Progress = 100`, and `StatusText = "Ready to download"` for non-own transfers;
  - set `Status = Available`, `Progress = 100`, and `StatusText = "Uploaded"` for own transfers.
- [ ] Add helper `MarkFileFailed(FileTransferFailedData failure)`:
  - set `Status = Failed`;
  - set `ErrorMessage = failure.Reason`;
  - set `StatusText = "Failed"`.
- [ ] Add `[RelayCommand] AttachFile`:
  - return when `_chatService.IsConnected == false`;
  - open `OpenFileDialog`;
  - reject files over `FileTransferItem.MaxFileSizeBytes` with `AddToast("File too large", "Maximum size is 1 GB.", "system", "file")`;
  - create `TransferId = Guid.NewGuid().ToString("N")`;
  - create and add local file card before upload starts;
  - call `_chatService.SendFileOfferAsync(offer)`;
  - start upload with `_ = UploadFileInBackgroundAsync(transfer, selectedPath)`.
- [ ] Add `UploadFileInBackgroundAsync(FileTransferItem transfer, string sourcePath)`:
  - create `CancellationTokenSource`;
  - call `_fileTransferService.UploadFileAsync`;
  - update progress through `Application.Current.Dispatcher.InvokeAsync`;
  - on success set `StatusText = "Uploaded"`;
  - on cancellation set `Status = Canceled`;
  - on failure set `Status = Failed`, `ErrorMessage`, and `StatusText = "Failed"`.
- [ ] Add `[RelayCommand] DownloadFile(FileTransferItem? transfer)`:
  - return when transfer is null or `CanDownload == false`;
  - open `SaveFileDialog` with `FileName = transfer.FileName`;
  - start `_ = DownloadFileInBackgroundAsync(transfer, destinationPath)`.
- [ ] Add `[RelayCommand] CancelFileTransfer(FileTransferItem? transfer)`:
  - find the transfer cancellation token by `TransferId`;
  - cancel it once;
  - set `StatusText = "Canceling"`.
- [ ] Add `DownloadFileInBackgroundAsync(FileTransferItem transfer, string destinationPath)` with the same progress and cleanup pattern as upload.
- [ ] Ensure `SendMessageCommand` and the message textbox are enabled based only on `IsConnected`, not file-transfer state.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck10
```

Expected output: `Build succeeded`.

## Task 11: Render File Cards And Attach Button In ChatView

File:

- `C:\Users\ADMIN\Documents\Chat\WpfChatClient\Views\ChatView.xaml`

Steps:

- [ ] In the composer grid, add one `Auto` column before the emoji button.
- [ ] Insert an Attach button after the textbox:

```xml
<Button Grid.Column="1"
        Style="{StaticResource ComposerIconButtonStyle}"
        Margin="0,0,8,0"
        ToolTip="Attach file"
        Command="{Binding AttachFileCommand}"
        IsEnabled="{Binding IsConnected}">
    <TextBlock Text="+" FontSize="22" FontWeight="SemiBold"/>
</Button>
```

- [ ] Move the emoji, sticker, and send buttons one column to the right.
- [ ] In the message template, keep the existing text and sticker UI.
- [ ] Add a `Border` below the sticker block with `Visibility` bound to `IsFile`; use a local style with a `DataTrigger`:

```xml
<Border Width="320"
        Padding="12"
        CornerRadius="14"
        BorderThickness="1"
        BorderBrush="{StaticResource BrightGlassBorder}"
        Background="#E6FFFFFF">
    <Border.Style>
        <Style TargetType="Border">
            <Setter Property="Visibility" Value="Collapsed"/>
            <Style.Triggers>
                <DataTrigger Binding="{Binding IsFile}" Value="True">
                    <Setter Property="Visibility" Value="Visible"/>
                </DataTrigger>
            </Style.Triggers>
        </Style>
    </Border.Style>
    <Grid DataContext="{Binding FileTransfer}">
        <Grid.RowDefinitions>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="Auto"/>
            <RowDefinition Height="Auto"/>
        </Grid.RowDefinitions>
        <Grid.ColumnDefinitions>
            <ColumnDefinition Width="Auto"/>
            <ColumnDefinition Width="*"/>
        </Grid.ColumnDefinitions>
        <Border Grid.RowSpan="2" Width="42" Height="42" CornerRadius="12" Background="{StaticResource LiquidAccentSoft}" Margin="0,0,10,0">
            <TextBlock Text="FILE" FontSize="10" FontWeight="Bold" Foreground="White" HorizontalAlignment="Center" VerticalAlignment="Center"/>
        </Border>
        <TextBlock Grid.Column="1" Text="{Binding FileName}" Foreground="{StaticResource BrightTextPrimary}" FontWeight="SemiBold" TextTrimming="CharacterEllipsis"/>
        <StackPanel Grid.Row="1" Grid.Column="1" Orientation="Horizontal" Margin="0,4,0,0">
            <TextBlock Text="{Binding SizeText}" Foreground="{StaticResource BrightTextSecondary}" FontSize="11"/>
            <TextBlock Text="  -  " Foreground="{StaticResource BrightTextSecondary}" FontSize="11"/>
            <TextBlock Text="{Binding StatusText}" Foreground="{StaticResource BrightTextSecondary}" FontSize="11"/>
        </StackPanel>
        <Grid Grid.Row="2" Grid.ColumnSpan="2" Margin="0,10,0,0">
            <Grid.ColumnDefinitions>
                <ColumnDefinition Width="*"/>
                <ColumnDefinition Width="Auto"/>
                <ColumnDefinition Width="Auto"/>
            </Grid.ColumnDefinitions>
            <ProgressBar Grid.Column="0" Minimum="0" Maximum="100" Height="6" Value="{Binding Progress}" VerticalAlignment="Center"/>
            <Button Grid.Column="1"
                    Content="Download"
                    Margin="10,0,0,0"
                    Command="{Binding DataContext.DownloadFileCommand, RelativeSource={RelativeSource AncestorType=ListBox}}"
                    CommandParameter="{Binding}">
                <Button.Style>
                    <Style TargetType="Button" BasedOn="{StaticResource FlatRowButtonStyle}">
                        <Setter Property="Visibility" Value="Collapsed"/>
                        <Style.Triggers>
                            <DataTrigger Binding="{Binding CanDownload}" Value="True">
                                <Setter Property="Visibility" Value="Visible"/>
                            </DataTrigger>
                        </Style.Triggers>
                    </Style>
                </Button.Style>
            </Button>
            <Button Grid.Column="2"
                    Content="Cancel"
                    Margin="8,0,0,0"
                    Command="{Binding DataContext.CancelFileTransferCommand, RelativeSource={RelativeSource AncestorType=ListBox}}"
                    CommandParameter="{Binding}">
                <Button.Style>
                    <Style TargetType="Button" BasedOn="{StaticResource FlatRowButtonStyle}">
                        <Setter Property="Visibility" Value="Collapsed"/>
                        <Style.Triggers>
                            <DataTrigger Binding="{Binding CanCancel}" Value="True">
                                <Setter Property="Visibility" Value="Visible"/>
                            </DataTrigger>
                        </Style.Triggers>
                    </Style>
                </Button.Style>
            </Button>
        </Grid>
    </Grid>
</Border>
```

- [ ] Confirm the file card does not overlap the existing sticker card and does not change the right-side online panel width.

Verification:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanCheck11
```

Expected output: `Build succeeded`.

## Task 12: Manual LAN And Stress Verification

Commands:

```powershell
dotnet run --project C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj
```

Open two more PowerShell windows:

```powershell
dotnet run --project C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj
dotnet run --project C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj
```

Create local test files:

```powershell
$small = "C:\Users\ADMIN\Documents\Chat\small-transfer-test.bin"
$large = "C:\Users\ADMIN\Documents\Chat\large-transfer-test.bin"
$fs = [System.IO.File]::Create($small); $fs.SetLength(1048576); $fs.Close()
$fs = [System.IO.File]::Create($large); $fs.SetLength(1073741824); $fs.Close()
```

Steps:

- [ ] Connect client A and client B to the host LAN IPv4 on port `5000`.
- [ ] Send normal text from both clients and confirm each message appears once.
- [ ] From client A, attach `small-transfer-test.bin`.
- [ ] Confirm client B sees a file card before download.
- [ ] While the small file is uploading, send text from client B and confirm it appears immediately.
- [ ] After upload finishes, click Download on client B and save the file.
- [ ] Confirm the downloaded file size is `1048576`:

```powershell
(Get-Item C:\Users\ADMIN\Documents\Chat\small-transfer-test.bin).Length
```

- [ ] From client A, attach `large-transfer-test.bin`.
- [ ] During the 1 GB upload, send at least five chat messages from client B; confirm the textbox remains clickable and messages arrive.
- [ ] During the 1 GB download, send at least five chat messages from client A; confirm messages arrive.
- [ ] Cancel an upload and confirm the matching `.part` file is removed from:

```powershell
C:\Users\ADMIN\Documents\Chat\ChatServer\bin\Debug\net9.0\ServerFiles
```

- [ ] Cancel a download and confirm the local `.part` file is removed from the selected download folder.
- [ ] Stop client A during an upload and confirm client B can still chat.
- [ ] Stop the server and confirm both clients show the existing reconnect/offline behavior without UI freeze.

## Task 13: Final Build And Git Check

Commands:

```powershell
dotnet build C:\Users\ADMIN\Documents\Chat\ChatServer\ChatServer.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\ChatServer\bin\FilePlanFinal
dotnet build C:\Users\ADMIN\Documents\Chat\WpfChatClient\WpfChatClient.csproj --no-restore -o C:\Users\ADMIN\Documents\Chat\WpfChatClient\bin\FilePlanFinal
git -C C:\Users\ADMIN\Documents\Chat status --short
```

Expected output:

- Both build commands end with `Build succeeded`.
- `git status --short` shows only the intended implementation files.

Commit command after implementation verification:

```powershell
git -C C:\Users\ADMIN\Documents\Chat add ChatServer WpfChatClient docs\superpowers\plans\2026-05-22-async-file-transfer-implementation.md
git -C C:\Users\ADMIN\Documents\Chat commit -m "Add async TCP file transfer"
```
