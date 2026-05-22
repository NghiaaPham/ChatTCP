using System;

namespace WpfChatClient.Core.Models;

public enum FileTransferDirection
{
    Outgoing,
    Incoming
}

public enum FileTransferStatus
{
    Pending,
    InProgress,
    Completed,
    Canceled,
    Failed
}

public sealed class FileTransferUpdate
{
    public string TransferKey { get; init; } = string.Empty;
    public string TransferId { get; init; } = string.Empty;
    public string RoomId { get; init; } = string.Empty;
    public string Peer { get; init; } = string.Empty;
    public string FileName { get; init; } = string.Empty;
    public long TotalBytes { get; init; }
    public long TransferredBytes { get; init; }
    public FileTransferDirection Direction { get; init; }
    public FileTransferStatus Status { get; init; }
    public string? Error { get; init; }
}
