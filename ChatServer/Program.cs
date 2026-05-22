using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace ChatServer
{
    // ==========================================
    // 1. PACKET PROTOCOL DEFINITIONS
    // ==========================================
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


    // ==========================================
    // 2. CLIENT SESSION MANAGEMENT
    // ==========================================
    public class ClientSession : IDisposable
    {
        public string SessionId { get; } = Guid.NewGuid().ToString();
        public string Username { get; set; } = "Anonymous";

        private readonly TcpClient _client;
        private readonly NetworkStream _stream;
        private readonly StreamReader _reader;
        private readonly StreamWriter _writer;
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private int _disconnectSignaled;
        private long _lastSeenTicks = DateTime.UtcNow.Ticks;

        public CancellationTokenSource Cts { get; } = new();
        public bool IsDisconnectSignaled => Volatile.Read(ref _disconnectSignaled) == 1;
        public DateTime LastSeenUtc => new(Interlocked.Read(ref _lastSeenTicks), DateTimeKind.Utc);

        public ClientSession(TcpClient client)
        {
            _client = client;
            _client.NoDelay = true;
            _stream = client.GetStream();
            _reader = new StreamReader(_stream, Encoding.UTF8);
            _writer = new StreamWriter(_stream, Encoding.UTF8) { AutoFlush = true };
        }

        public void MarkSeen()
        {
            Interlocked.Exchange(ref _lastSeenTicks, DateTime.UtcNow.Ticks);
        }

        public async Task<string?> ReadLineAsync()
        {
            return await _reader.ReadLineAsync(Cts.Token);
        }

        public async Task<bool> SendPacketAsync(Packet packet)
        {
            if (Cts.IsCancellationRequested) return false;

            var lockTaken = false;
            try
            {
                await _writeLock.WaitAsync(Cts.Token);
                lockTaken = true;

                var json = JsonSerializer.Serialize(packet);
                await _writer.WriteLineAsync(json);
                await _writer.FlushAsync();
                MarkSeen();
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (Exception ex)
            {
                RequestDisconnect($"send failed: {ex.GetType().Name}: {ex.Message}");
                return false;
            }
            finally
            {
                if (lockTaken) _writeLock.Release();
            }
        }

        public void RequestDisconnect(string reason)
        {
            if (Interlocked.Exchange(ref _disconnectSignaled, 1) == 1)
            {
                return;
            }

            try { Cts.Cancel(); } catch { }
            try { _stream.Close(); } catch { }
            try { _client.Close(); } catch { }
            Console.WriteLine($"[SERVER] disconnect signaled ({Username}, {SessionId}): {reason}");
        }

        public void Dispose()
        {
            RequestDisconnect("dispose");
            SafeDispose(_reader);
            SafeDispose(_writer);
            SafeDispose(_stream);
            SafeDispose(_client);
            SafeDispose(Cts);
            SafeDispose(_writeLock);
        }

        private static void SafeDispose(IDisposable? disposable)
        {
            try
            {
                disposable?.Dispose();
            }
            catch
            {
            }
        }
    }


    // ==========================================
    // 3. THREAD-SAFE CONNECTED CLIENTS MANAGER
    // ==========================================
    public class ConnectedClients
    {
        private readonly ConcurrentDictionary<string, ClientSession> _sessions = new();
        private readonly ConcurrentDictionary<string, string> _userSessions = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, HashSet<string>> _roomMembers = new();

        public void Add(ClientSession session)
        {
            _sessions.TryAdd(session.SessionId, session);
        }

        public bool Remove(string sessionId)
        {
            if (!_sessions.TryRemove(sessionId, out var session))
            {
                return false;
            }

            if (session.Username != "Anonymous" &&
                _userSessions.TryGetValue(session.Username, out var mappedSessionId) &&
                mappedSessionId == sessionId)
            {
                _userSessions.TryRemove(session.Username, out _);
            }

            // Remove from all rooms
            foreach (var room in _roomMembers)
            {
                lock (room.Value)
                {
                    room.Value.Remove(sessionId);
                }
            }

            return true;
        }

        public bool TryRegisterUsername(ClientSession session, string username)
        {
            username = username.Trim();
            if (string.IsNullOrWhiteSpace(username)) return false;

            if (session.Username.Equals(username, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (session.Username != "Anonymous")
            {
                return false;
            }

            if (_userSessions.TryGetValue(username, out var existingSessionId))
            {
                if (existingSessionId == session.SessionId)
                {
                    return true;
                }

                if (!_sessions.TryGetValue(existingSessionId, out var existingSession) ||
                    existingSession.IsDisconnectSignaled)
                {
                    Remove(existingSessionId);
                }
                else
                {
                    return false;
                }
            }

            if (!_userSessions.TryAdd(username, session.SessionId))
            {
                return false;
            }

            session.Username = username;
            return true;
        }

        public void AddToRoom(string sessionId, string roomId)
        {
            var members = _roomMembers.GetOrAdd(roomId, _ => new HashSet<string>());
            lock (members)
            {
                members.Add(sessionId);
            }
        }

        public void RemoveFromRoom(string sessionId, string roomId)
        {
            if (_roomMembers.TryGetValue(roomId, out var members))
            {
                lock (members)
                {
                    members.Remove(sessionId);
                }
            }
        }

        public ClientSession? GetByUsername(string username)
        {
            return _sessions.Values.FirstOrDefault(s => s.Username.Equals(username, StringComparison.OrdinalIgnoreCase));
        }

        public IEnumerable<ClientSession> GetAll()
        {
            return _sessions.Values;
        }

        public int SessionCount => _sessions.Count;
        public int UserCount => _userSessions.Count;

        public async Task BroadcastAsync(Packet packet, string? excludeSessionId = null)
        {
            var tasks = new List<Task>();
            foreach (var session in _sessions.Values)
            {
                if (excludeSessionId == null || session.SessionId != excludeSessionId)
                {
                    tasks.Add(session.SendPacketAsync(packet));
                }
            }
            await Task.WhenAll(tasks);
        }

        public async Task BroadcastToRoomAsync(string roomId, Packet packet, string? excludeSessionId = null)
        {
            if (!_roomMembers.TryGetValue(roomId, out var members)) return;

            List<string> sessionIds;
            lock (members)
            {
                sessionIds = new List<string>(members);
            }

            var tasks = new List<Task>();
            foreach (var sessionId in sessionIds)
            {
                if (excludeSessionId != null && sessionId == excludeSessionId) continue;
                if (_sessions.TryGetValue(sessionId, out var session))
                {
                    tasks.Add(session.SendPacketAsync(packet));
                }
            }
            await Task.WhenAll(tasks);
        }
    }


    // ==========================================
    // 4. MESSAGE ROUTER
    // ==========================================
    public class RoomFileStore
    {
        private readonly object _syncRoot = new();
        private readonly string _rootPath;
        private readonly Dictionary<string, List<RoomFileDescriptor>> _roomFiles = new(StringComparer.OrdinalIgnoreCase);

        public RoomFileStore(string rootPath)
        {
            _rootPath = rootPath;
        }

        public string GetRoomPath(string roomId)
        {
            return Path.Combine(_rootPath, SanitizeRoomId(roomId));
        }

        public List<RoomFileDescriptor> GetRoomFiles(string roomId)
        {
            lock (_syncRoot)
            {
                return new List<RoomFileDescriptor>(GetRoomFilesLocked(roomId));
            }
        }

        public RoomFileDescriptor? GetRoomFile(string roomId, string fileId)
        {
            lock (_syncRoot)
            {
                var files = GetRoomFilesLocked(roomId);
                return files.FirstOrDefault(f => f.FileId.Equals(fileId, StringComparison.OrdinalIgnoreCase));
            }
        }

        public void AddRoomFile(string roomId, RoomFileDescriptor descriptor)
        {
            lock (_syncRoot)
            {
                var files = GetRoomFilesLocked(roomId);
                files.RemoveAll(f => f.FileId.Equals(descriptor.FileId, StringComparison.OrdinalIgnoreCase));
                files.Insert(0, descriptor);
                SaveRoomIndexLocked(roomId, files);
            }
        }

        private List<RoomFileDescriptor> GetRoomFilesLocked(string roomId)
        {
            if (_roomFiles.TryGetValue(roomId, out var files)) return files;

            var loaded = LoadRoomIndexLocked(roomId);
            _roomFiles[roomId] = loaded;
            return loaded;
        }

        private List<RoomFileDescriptor> LoadRoomIndexLocked(string roomId)
        {
            var roomPath = GetRoomPath(roomId);
            var indexPath = Path.Combine(roomPath, "room-index.json");
            if (!File.Exists(indexPath)) return new List<RoomFileDescriptor>();

            try
            {
                var json = File.ReadAllText(indexPath);
                return JsonSerializer.Deserialize<List<RoomFileDescriptor>>(json) ?? new List<RoomFileDescriptor>();
            }
            catch
            {
                return new List<RoomFileDescriptor>();
            }
        }

        private void SaveRoomIndexLocked(string roomId, List<RoomFileDescriptor> files)
        {
            var roomPath = GetRoomPath(roomId);
            Directory.CreateDirectory(roomPath);
            var indexPath = Path.Combine(roomPath, "room-index.json");
            var json = JsonSerializer.Serialize(files, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(indexPath, json);
        }

        private static string SanitizeRoomId(string roomId)
        {
            var invalid = Path.GetInvalidFileNameChars();
            var clean = new string(roomId.Select(c => invalid.Contains(c) ? '_' : c).ToArray());
            return string.IsNullOrWhiteSpace(clean) ? "General" : clean;
        }
    }

    public class RoomFileUploadSession
    {
        public string TransferId { get; init; } = string.Empty;
        public string RoomId { get; init; } = string.Empty;
        public string Sender { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public long FileSize { get; init; }
        public string TempPath { get; init; } = string.Empty;
        public long ReceivedBytes { get; set; }
        public object SyncRoot { get; } = new();
    }

    public class MessageRouter
    {
        private readonly ConnectedClients _clients;
        private readonly RoomFileStore _roomFiles;
        private readonly Dictionary<string, RoomFileUploadSession> _uploadSessions = new(StringComparer.OrdinalIgnoreCase);
        private const int FileChunkSize = 64 * 1024;

        public MessageRouter(ConnectedClients clients, RoomFileStore roomFiles)
        {
            _clients = clients;
            _roomFiles = roomFiles;
        }

        public async Task RouteAsync(ClientSession session, string jsonPayload)
        {
            try
            {
                var packet = JsonSerializer.Deserialize<Packet>(jsonPayload);
                if (packet == null) return;

                switch (packet.Type)
                {
                    case PacketType.Join:
                        var joinData = packet.Data.Deserialize<JoinData>();
                        if (joinData != null)
                        {
                            var requestedUsername = joinData.Username.Trim();
                            bool isFirstJoin = session.Username == "Anonymous";
                            if (!_clients.TryRegisterUsername(session, requestedUsername))
                            {
                                LogWarning($"[DUPLICATE] rejected session {session.SessionId} for username '{requestedUsername}'");
                                await session.SendPacketAsync(new Packet
                                {
                                    Type = PacketType.ConnectionRejected,
                                    Data = JsonSerializer.SerializeToElement(new SystemMessageData
                                    {
                                        Message = "This username already has an active session."
                                    })
                                });
                                session.RequestDisconnect("duplicate username");
                                return;
                            }

                            if (isFirstJoin)
                            {
                                LogSuccess($"[+] {session.Username} joined (Session: {session.SessionId})");
                                LogInfo($"[SERVER] session created: {session.SessionId} | Active sessions: {_clients.SessionCount} | Active users: {_clients.UserCount}");
                                await BroadcastSystemMessageAsync($"{session.Username} has joined the chat.");
                            }
                            else
                            {
                                LogInfo($"[JOIN] duplicate join packet ignored for {session.Username}");
                            }

                            await BroadcastUserListAsync();
                        }
                        break;

                    case PacketType.ChatMessage:
                        var chatData = packet.Data.Deserialize<ChatMessageData>();
                        if (chatData != null)
                        {
                            string timestamp = DateTime.Now.ToString("o"); // ISO 8601
                            string messageId = string.IsNullOrWhiteSpace(chatData.MessageId) ? Guid.NewGuid().ToString("N") : chatData.MessageId;
                            string roomId = string.IsNullOrWhiteSpace(chatData.RoomId) ? "General" : chatData.RoomId;
                            LogMessage($"[{DateTime.Now:HH:mm:ss}] [{roomId}] {session.Username}: {chatData.Content}");

                            var responsePacket = new Packet
                            {
                                Type = PacketType.ChatMessage,
                                Data = JsonSerializer.SerializeToElement(new ChatMessageData
                                {
                                    MessageId = messageId,
                                    RoomId = roomId,
                                    Username = session.Username,
                                    Content = chatData.Content,
                                    Timestamp = timestamp
                                })
                            };

                            if (roomId.Equals("General", StringComparison.OrdinalIgnoreCase))
                            {
                                await _clients.BroadcastAsync(responsePacket);
                            }
                            else
                            {
                                await _clients.BroadcastToRoomAsync(roomId, responsePacket);
                            }
                        }
                        break;

                    case PacketType.PrivateMessage:
                        var privateData = packet.Data.Deserialize<PrivateMessageData>();
                        if (privateData != null)
                        {
                            string timestamp = DateTime.Now.ToString("o");
                            LogMessage($"[{DateTime.Now:HH:mm:ss}] [DM] {session.Username} -> {privateData.Recipient}: {privateData.Content}");

                            var recipientSession = _clients.GetByUsername(privateData.Recipient);
                            if (recipientSession != null)
                            {
                                await recipientSession.SendPacketAsync(new Packet
                                {
                                    Type = PacketType.PrivateMessage,
                                    Data = JsonSerializer.SerializeToElement(new PrivateMessageData
                                    {
                                        MessageId = privateData.MessageId,
                                        Sender = session.Username,
                                        Recipient = privateData.Recipient,
                                        Content = privateData.Content,
                                        Timestamp = timestamp
                                    })
                                });
                            }
                        }
                        break;

                    case PacketType.RoomJoin:
                        var roomJoinData = packet.Data.Deserialize<RoomJoinData>();
                        if (roomJoinData != null)
                        {
                            LogInfo($"[ROOM] {session.Username} joined room {roomJoinData.RoomId}");
                            _clients.AddToRoom(session.SessionId, roomJoinData.RoomId);

                            await session.SendPacketAsync(new Packet
                            {
                                Type = PacketType.SystemMessage,
                                Data = JsonSerializer.SerializeToElement(new SystemMessageData { Message = $"You joined room: {roomJoinData.RoomId}" })
                            });

                            await SendRoomFileListAsync(session, roomJoinData.RoomId);
                        }
                        break;

                    case PacketType.Typing:
                        var typingData = packet.Data.Deserialize<TypingData>();
                        if (typingData != null)
                        {
                            // Broadcast typing status to everyone EXCEPT the person typing
                            await _clients.BroadcastAsync(new Packet
                            {
                                Type = PacketType.Typing,
                                Data = JsonSerializer.SerializeToElement(new TypingData
                                {
                                    Username = session.Username,
                                    IsTyping = typingData.IsTyping
                                })
                            }, excludeSessionId: session.SessionId);
                        }
                        break;

                    case PacketType.Heartbeat:
                        // Optionally respond with a Heartbeat packet to confirm the server is alive
                        await session.SendPacketAsync(new Packet
                        {
                            Type = PacketType.Heartbeat,
                            Data = JsonSerializer.SerializeToElement(new HeartbeatData())
                        });
                        break;

                    case PacketType.FileTransferRequest:
                        var requestData = packet.Data.Deserialize<FileTransferRequestData>();
                        if (requestData != null)
                        {
                            var roomId = string.IsNullOrWhiteSpace(requestData.RoomId) ? "General" : requestData.RoomId;
                            var forwardPacket = new Packet
                            {
                                Type = PacketType.FileTransferRequest,
                                Data = JsonSerializer.SerializeToElement(new FileTransferRequestData
                                {
                                    TransferId = requestData.TransferId,
                                    RoomId = roomId,
                                    Sender = session.Username,
                                    FileName = requestData.FileName,
                                    FileSize = requestData.FileSize
                                })
                            };

                            if (roomId.Equals("General", StringComparison.OrdinalIgnoreCase))
                            {
                                await _clients.BroadcastAsync(forwardPacket, excludeSessionId: null);
                            }
                            else
                            {
                                await _clients.BroadcastToRoomAsync(roomId, forwardPacket, excludeSessionId: null);
                            }
                        }
                        break;

                    case PacketType.FileTransferResume:
                        var resumeData = packet.Data.Deserialize<FileTransferResumeData>();
                        if (resumeData != null)
                        {
                            var senderSession = _clients.GetByUsername(resumeData.Sender);
                            if (senderSession != null)
                            {
                                await senderSession.SendPacketAsync(new Packet
                                {
                                    Type = PacketType.FileTransferResume,
                                    Data = JsonSerializer.SerializeToElement(new FileTransferResumeData
                                    {
                                        TransferId = resumeData.TransferId,
                                        RoomId = resumeData.RoomId,
                                        Sender = resumeData.Sender,
                                        Recipient = session.Username,
                                        ReceivedBytes = resumeData.ReceivedBytes
                                    })
                                });
                            }
                        }
                        break;

                    case PacketType.FileTransferChunk:
                        var chunkData = packet.Data.Deserialize<FileTransferChunkData>();
                        if (chunkData != null)
                        {
                            var roomId = string.IsNullOrWhiteSpace(chunkData.RoomId) ? "General" : chunkData.RoomId;
                            var forwardChunk = new Packet
                            {
                                Type = PacketType.FileTransferChunk,
                                Data = JsonSerializer.SerializeToElement(new FileTransferChunkData
                                {
                                    TransferId = chunkData.TransferId,
                                    RoomId = roomId,
                                    Sender = session.Username,
                                    Recipient = chunkData.Recipient,
                                    Offset = chunkData.Offset,
                                    DataBase64 = chunkData.DataBase64,
                                    IsLast = chunkData.IsLast
                                })
                            };

                            if (!string.IsNullOrWhiteSpace(chunkData.Recipient))
                            {
                                var recipientSession = _clients.GetByUsername(chunkData.Recipient);
                                if (recipientSession != null)
                                {
                                    await recipientSession.SendPacketAsync(forwardChunk);
                                }
                            }
                            else if (roomId.Equals("General", StringComparison.OrdinalIgnoreCase))
                            {
                                await _clients.BroadcastAsync(forwardChunk, excludeSessionId: session.SessionId);
                            }
                            else
                            {
                                await _clients.BroadcastToRoomAsync(roomId, forwardChunk, excludeSessionId: session.SessionId);
                            }
                        }
                        break;

                    case PacketType.FileTransferCancel:
                        var cancelData = packet.Data.Deserialize<FileTransferCancelData>();
                        if (cancelData != null)
                        {
                            var roomId = string.IsNullOrWhiteSpace(cancelData.RoomId) ? "General" : cancelData.RoomId;

                            if (!string.IsNullOrWhiteSpace(cancelData.Sender) &&
                                !cancelData.Sender.Equals(session.Username, StringComparison.OrdinalIgnoreCase))
                            {
                                var senderSession = _clients.GetByUsername(cancelData.Sender);
                                if (senderSession != null)
                                {
                                    await senderSession.SendPacketAsync(new Packet
                                    {
                                        Type = PacketType.FileTransferCancel,
                                        Data = JsonSerializer.SerializeToElement(new FileTransferCancelData
                                        {
                                            TransferId = cancelData.TransferId,
                                            RoomId = roomId,
                                            Sender = cancelData.Sender,
                                            Recipient = session.Username,
                                            Reason = cancelData.Reason
                                        })
                                    });
                                }

                                break;
                            }

                            if (!string.IsNullOrWhiteSpace(cancelData.Recipient))
                            {
                                var recipientSession = _clients.GetByUsername(cancelData.Recipient);
                                if (recipientSession != null)
                                {
                                    await recipientSession.SendPacketAsync(packet);
                                }
                            }
                            else if (roomId.Equals("General", StringComparison.OrdinalIgnoreCase))
                            {
                                await _clients.BroadcastAsync(packet, excludeSessionId: session.SessionId);
                            }
                            else
                            {
                                await _clients.BroadcastToRoomAsync(roomId, packet, excludeSessionId: session.SessionId);
                            }
                        }
                        break;

                    case PacketType.RoomFileUploadRequest:
                        var uploadRequest = packet.Data.Deserialize<RoomFileUploadRequestData>();
                        if (uploadRequest != null)
                        {
                            await HandleRoomFileUploadRequestAsync(session, uploadRequest);
                        }
                        break;

                    case PacketType.RoomFileUploadChunk:
                        var uploadChunk = packet.Data.Deserialize<RoomFileUploadChunkData>();
                        if (uploadChunk != null)
                        {
                            await HandleRoomFileUploadChunkAsync(session, uploadChunk);
                        }
                        break;

                    case PacketType.RoomFileDownloadRequest:
                        var downloadRequest = packet.Data.Deserialize<RoomFileDownloadRequestData>();
                        if (downloadRequest != null)
                        {
                            await HandleRoomFileDownloadRequestAsync(session, downloadRequest);
                        }
                        break;

                    default:
                        LogWarning($"[!] Unhandled packet type received: {packet.Type}");
                        break;
                }
            }
            catch (JsonException)
            {
                LogError($"[ERROR] Failed to parse incoming JSON from {session.Username}");
            }
            catch (Exception ex)
            {
                LogError($"[ERROR] Unexpected error routing packet: {ex.Message}");
            }
        }

        private async Task SendRoomFileListAsync(ClientSession session, string roomId)
        {
            var normalizedRoomId = string.IsNullOrWhiteSpace(roomId) ? "General" : roomId;
            var files = _roomFiles.GetRoomFiles(normalizedRoomId);

            await session.SendPacketAsync(new Packet
            {
                Type = PacketType.RoomFileList,
                Data = JsonSerializer.SerializeToElement(new RoomFileListData
                {
                    RoomId = normalizedRoomId,
                    Files = files
                })
            });
        }

        private async Task BroadcastRoomFileListAsync(string roomId)
        {
            var normalizedRoomId = string.IsNullOrWhiteSpace(roomId) ? "General" : roomId;
            var files = _roomFiles.GetRoomFiles(normalizedRoomId);

            await _clients.BroadcastToRoomAsync(normalizedRoomId, new Packet
            {
                Type = PacketType.RoomFileList,
                Data = JsonSerializer.SerializeToElement(new RoomFileListData
                {
                    RoomId = normalizedRoomId,
                    Files = files
                })
            });
        }

        private async Task HandleRoomFileUploadRequestAsync(ClientSession session, RoomFileUploadRequestData request)
        {
            var roomId = string.IsNullOrWhiteSpace(request.RoomId) ? "General" : request.RoomId;
            var roomPath = _roomFiles.GetRoomPath(roomId);
            Directory.CreateDirectory(roomPath);

            var safeFileName = Path.GetFileName(request.FileName);
            var tempPath = Path.Combine(roomPath, $"{request.TransferId}_{safeFileName}.part");
            long existingBytes = 0;
            if (File.Exists(tempPath))
            {
                existingBytes = new FileInfo(tempPath).Length;
            }

            var uploadSession = new RoomFileUploadSession
            {
                TransferId = request.TransferId,
                RoomId = roomId,
                Sender = session.Username,
                FileName = safeFileName,
                FileSize = request.FileSize,
                TempPath = tempPath,
                ReceivedBytes = existingBytes
            };

            lock (_uploadSessions)
            {
                _uploadSessions[request.TransferId] = uploadSession;
            }

            await session.SendPacketAsync(new Packet
            {
                Type = PacketType.RoomFileUploadResume,
                Data = JsonSerializer.SerializeToElement(new RoomFileUploadResumeData
                {
                    TransferId = request.TransferId,
                    RoomId = roomId,
                    ReceivedBytes = existingBytes
                })
            });
        }

        private async Task HandleRoomFileUploadChunkAsync(ClientSession session, RoomFileUploadChunkData chunk)
        {
            RoomFileUploadSession? uploadSession;
            lock (_uploadSessions)
            {
                _uploadSessions.TryGetValue(chunk.TransferId, out uploadSession);
            }

            if (uploadSession == null) return;

            byte[] payload;
            try
            {
                payload = Convert.FromBase64String(chunk.DataBase64);
            }
            catch
            {
                return;
            }

            bool completed = false;
            long receivedBytes;

            lock (uploadSession.SyncRoot)
            {
                if (chunk.Offset < uploadSession.ReceivedBytes)
                {
                    return;
                }

                if (chunk.Offset > uploadSession.ReceivedBytes)
                {
                    return;
                }

                using var stream = new FileStream(uploadSession.TempPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                stream.Seek(uploadSession.ReceivedBytes, SeekOrigin.Begin);
                stream.Write(payload, 0, payload.Length);
                uploadSession.ReceivedBytes += payload.Length;
                receivedBytes = uploadSession.ReceivedBytes;

                if (chunk.IsLast || uploadSession.ReceivedBytes >= uploadSession.FileSize)
                {
                    completed = true;
                }
            }

            if (completed)
            {
                var roomPath = _roomFiles.GetRoomPath(uploadSession.RoomId);
                Directory.CreateDirectory(roomPath);
                var finalName = $"{uploadSession.TransferId}_{uploadSession.FileName}";
                var finalPath = Path.Combine(roomPath, finalName);

                try
                {
                    File.Move(uploadSession.TempPath, finalPath, overwrite: true);
                }
                catch
                {
                }

                var descriptor = new RoomFileDescriptor
                {
                    FileId = uploadSession.TransferId,
                    FileName = uploadSession.FileName,
                    FileSize = uploadSession.FileSize,
                    UploadedBy = uploadSession.Sender,
                    UploadedAt = DateTime.Now.ToString("o")
                };

                _roomFiles.AddRoomFile(uploadSession.RoomId, descriptor);

                lock (_uploadSessions)
                {
                    _uploadSessions.Remove(uploadSession.TransferId);
                }

                await BroadcastRoomFileListAsync(uploadSession.RoomId);
                LogInfo($"[ROOM FILE] Uploaded {descriptor.FileName} ({descriptor.FileSize} bytes) to {uploadSession.RoomId}");
            }
        }

        private async Task HandleRoomFileDownloadRequestAsync(ClientSession session, RoomFileDownloadRequestData request)
        {
            var roomId = string.IsNullOrWhiteSpace(request.RoomId) ? "General" : request.RoomId;
            var file = _roomFiles.GetRoomFile(roomId, request.FileId);
            if (file == null) return;

            var roomPath = _roomFiles.GetRoomPath(roomId);
            var filePath = Path.Combine(roomPath, $"{file.FileId}_{file.FileName}");
            if (!File.Exists(filePath)) return;

            using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            var offset = Math.Max(0, request.Offset);
            if (offset > stream.Length) offset = stream.Length;
            stream.Seek(offset, SeekOrigin.Begin);

            var buffer = new byte[FileChunkSize];
            long sentBytes = offset;

            while (sentBytes < stream.Length)
            {
                int read = await stream.ReadAsync(buffer, 0, buffer.Length).ConfigureAwait(false);
                if (read <= 0) break;

                var payload = Convert.ToBase64String(buffer, 0, read);
                var isLast = sentBytes + read >= stream.Length;

                await session.SendPacketAsync(new Packet
                {
                    Type = PacketType.RoomFileDownloadChunk,
                    Data = JsonSerializer.SerializeToElement(new RoomFileDownloadChunkData
                    {
                        FileId = file.FileId,
                        RoomId = roomId,
                        Offset = sentBytes,
                        DataBase64 = payload,
                        IsLast = isLast,
                        Descriptor = file
                    })
                });

                sentBytes += read;
            }
        }

        public async Task BroadcastUserListAsync()
        {
            var users = new HashSet<string>();
            foreach (var s in _clients.GetAll())
            {
                if (s.Username != "Anonymous") users.Add(s.Username);
            }

            await _clients.BroadcastAsync(new Packet
            {
                Type = PacketType.UserListUpdate,
                Data = JsonSerializer.SerializeToElement(new UserListUpdateData { Users = new List<string>(users) })
            });
        }

        public async Task BroadcastSystemMessageAsync(string message)
        {
            await _clients.BroadcastAsync(new Packet
            {
                Type = PacketType.SystemMessage,
                Data = JsonSerializer.SerializeToElement(new SystemMessageData { Message = message })
            });
        }

        private static void LogSuccess(string msg) { Console.ForegroundColor = ConsoleColor.Green; Console.WriteLine(msg); Console.ResetColor(); }
        private static void LogInfo(string msg) { Console.ForegroundColor = ConsoleColor.White; Console.WriteLine(msg); Console.ResetColor(); }
        private static void LogMessage(string msg) { Console.ForegroundColor = ConsoleColor.Cyan; Console.WriteLine(msg); Console.ResetColor(); }
        private static void LogWarning(string msg) { Console.ForegroundColor = ConsoleColor.Yellow; Console.WriteLine(msg); Console.ResetColor(); }
        private static void LogError(string msg) { Console.ForegroundColor = ConsoleColor.Red; Console.WriteLine(msg); Console.ResetColor(); }
    }


    // ==========================================
    // 5. SERVER ENTRY POINT
    // ==========================================
    class Program
    {
        private static readonly ConnectedClients _clients = new();
        private static readonly RoomFileStore _roomFiles = new(Path.Combine(AppContext.BaseDirectory, "room-files"));
        private static readonly MessageRouter _router = new(_clients, _roomFiles);
        private static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(75);
        private static readonly TimeSpan HeartbeatCheckInterval = TimeSpan.FromSeconds(10);

        static async Task Main(string[] args)
        {
            int port = 5000;
            TcpListener listener = new(IPAddress.Any, port);

            try
            {
                listener.Start();
                PrintBanner(port);

                while (true)
                {
                    // Fully asynchronous accept
                    var tcpClient = await listener.AcceptTcpClientAsync();

                    // Fire and forget handling to prevent blocking the accept loop
                    _ = HandleClientAsync(tcpClient);
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[CRITICAL] Server failed: {ex.Message}");
                Console.ResetColor();
            }
        }

        static async Task HandleClientAsync(TcpClient tcpClient)
        {
            using var session = new ClientSession(tcpClient);
            _clients.Add(session);
            Console.WriteLine($"[SERVER] session accepted: {session.SessionId} | Active sessions: {_clients.SessionCount}");
            var heartbeatWatchdog = Task.Run(() => HeartbeatWatchdogAsync(session));

            try
            {
                while (!session.Cts.Token.IsCancellationRequested)
                {
                    // Await incoming data asynchronously. If client drops abruptly, this may throw.
                    string? line = await session.ReadLineAsync();

                    // A null line indicates the client cleanly closed the connection.
                    if (line == null) break;

                    session.MarkSeen();

                    await _router.RouteAsync(session, line);
                }
            }
            catch (OperationCanceledException) { /* Session was intentionally stopped */ }
            catch (IOException) { /* Client forcibly closed connection */ }
            catch (SocketException) { /* Socket dropped */ }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[ERROR] Session exception ({session.Username}): {ex.Message}");
                Console.ResetColor();
            }
            finally
            {
                // Cleanup on disconnect
                session.RequestDisconnect("handler ended");
                bool removed = _clients.Remove(session.SessionId);

                if (removed)
                {
                    Console.WriteLine($"[SERVER] session removed: {session.SessionId} | Active sessions: {_clients.SessionCount} | Active users: {_clients.UserCount}");
                }

                if (removed && session.Username != "Anonymous")
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"[-] {session.Username} disconnected | Online: {_clients.UserCount}");
                    Console.ResetColor();

                    await _router.BroadcastSystemMessageAsync($"{session.Username} has left the chat.");
                    await _router.BroadcastUserListAsync();
                }

                try { await heartbeatWatchdog; } catch { }
            }
        }

        static async Task HeartbeatWatchdogAsync(ClientSession session)
        {
            try
            {
                while (!session.Cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(HeartbeatCheckInterval, session.Cts.Token);
                    if (DateTime.UtcNow - session.LastSeenUtc > HeartbeatTimeout)
                    {
                        session.RequestDisconnect("heartbeat timeout");
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        static void PrintBanner(int port)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("========================================");
            Console.WriteLine("        CLASSROOM TCP CHAT SERVER       ");
            Console.WriteLine("========================================");
            Console.ResetColor();
            Console.WriteLine($"Server is listening on TCP port {port}");
            Console.WriteLine();
            Console.WriteLine("Give classmates your Wi-Fi/LAN IPv4 address below, not 127.0.0.1:");
            var addresses = GetLocalIPv4Addresses();
            if (addresses.Count == 0)
            {
                Console.WriteLine("  No active LAN IPv4 address found. Run ipconfig and check your Wi-Fi adapter.");
            }
            else
            {
                foreach (var address in addresses)
                {
                    Console.WriteLine($"  {address.InterfaceName}: {address.Address}");
                }
            }
            Console.WriteLine();
            Console.WriteLine("Waiting for TCP clients...\n");
        }

        static List<(string InterfaceName, string Address)> GetLocalIPv4Addresses()
        {
            var addresses = new List<(string InterfaceName, string Address)>();
            NetworkInterface[] networkInterfaces;
            try
            {
                networkInterfaces = NetworkInterface.GetAllNetworkInterfaces();
            }
            catch (NetworkInformationException)
            {
                return addresses;
            }

            foreach (var networkInterface in networkInterfaces)
            {
                if (networkInterface.OperationalStatus != OperationalStatus.Up ||
                    networkInterface.NetworkInterfaceType == NetworkInterfaceType.Loopback ||
                    networkInterface.NetworkInterfaceType == NetworkInterfaceType.Tunnel)
                {
                    continue;
                }
                IPInterfaceProperties properties;
                try
                {
                    properties = networkInterface.GetIPProperties();
                }
                catch (NetworkInformationException)
                {
                    continue;
                }

                foreach (var unicastAddress in properties.UnicastAddresses)
                {
                    if (unicastAddress.Address.AddressFamily == AddressFamily.InterNetwork &&
                        !IPAddress.IsLoopback(unicastAddress.Address))
                    {
                        addresses.Add((networkInterface.Name, unicastAddress.Address.ToString()));
                    }
                }
            }
            return addresses;
        }
    }
}
