using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace WpfChatClient.Core.Models;

public enum PacketType
{
    Join,
    Leave,
    ChatMessage,
    Typing,
    UserListUpdate,
    SystemMessage,
    Heartbeat,
    PrivateMessage,
    RoomJoin,
    ConnectionRejected,
    FileTransferRequest,
    FileTransferChunk,
    FileTransferResume,
    FileTransferCancel,
    RoomFileUploadRequest,
    RoomFileUploadResume,
    RoomFileUploadChunk,
    RoomFileList,
    RoomFileDownloadRequest,
    RoomFileDownloadChunk
}

public class Packet
{
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public PacketType Type { get; set; }
    public JsonElement Data { get; set; }
}

public class JoinData { public string Username { get; set; } = string.Empty; }

public class ChatMessageData
{
    public string MessageId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Timestamp { get; set; } = string.Empty;
}

public class PrivateMessageData
{
    public string MessageId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Timestamp { get; set; } = string.Empty;
}

public class RoomJoinData
{
    public string RoomId { get; set; } = string.Empty;
}

public class TypingData
{
    public string Username { get; set; } = string.Empty;
    public bool IsTyping { get; set; }
}

public class UserListUpdateData { public List<string> Users { get; set; } = new(); }
public class SystemMessageData { public string Message { get; set; } = string.Empty; }
public class HeartbeatData { }

public class FileTransferRequestData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
}

public class FileTransferChunkData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public long Offset { get; set; }
    public string DataBase64 { get; set; } = string.Empty;
    public bool IsLast { get; set; }
}

public class FileTransferResumeData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public long ReceivedBytes { get; set; }
}

public class FileTransferCancelData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
}

public class RoomFileDescriptor
{
    public string FileId { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
    public string UploadedBy { get; set; } = string.Empty;
    public string UploadedAt { get; set; } = string.Empty;
}

public class RoomFileListData
{
    public string RoomId { get; set; } = string.Empty;
    public List<RoomFileDescriptor> Files { get; set; } = new();
}

public class RoomFileUploadRequestData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public string Sender { get; set; } = string.Empty;
    public string FileName { get; set; } = string.Empty;
    public long FileSize { get; set; }
}

public class RoomFileUploadResumeData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public long ReceivedBytes { get; set; }
}

public class RoomFileUploadChunkData
{
    public string TransferId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public long Offset { get; set; }
    public string DataBase64 { get; set; } = string.Empty;
    public bool IsLast { get; set; }
}

public class RoomFileDownloadRequestData
{
    public string FileId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public long Offset { get; set; }
}

public class RoomFileDownloadChunkData
{
    public string FileId { get; set; } = string.Empty;
    public string RoomId { get; set; } = string.Empty;
    public long Offset { get; set; }
    public string DataBase64 { get; set; } = string.Empty;
    public bool IsLast { get; set; }
    public RoomFileDescriptor Descriptor { get; set; } = new();
}
