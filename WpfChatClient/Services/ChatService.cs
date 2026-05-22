using System;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using WpfChatClient.Core.Models;
using WpfChatClient.Core.Interfaces;
using System.Collections.Concurrent;
using System.Collections.Generic;
using WpfChatClient.Infrastructure;

namespace WpfChatClient.Services;

public class ChatService : IChatService
{
    private TcpClient? _client;
    private NetworkStream? _stream;
    private StreamReader? _reader;
    private StreamWriter? _writer;
    private CancellationTokenSource? _cts;
    private CancellationTokenSource? _reconnectCts;
    private Task? _receiveTask;
    private Task? _heartbeatTask;
    private Task? _reconnectTask;
    private string? _lastIp;
    private int _lastPort;
    private string? _lastUsername;
    private volatile bool _isConnecting;
    private volatile bool _isConnected;
    private volatile bool _isReconnectLoopRunning;
    private volatile bool _isIntentionallyDisconnected = true;
    private string _activeRoomId = "General";
    private int _connectionId;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly MessageCache _messageCache;
    private readonly ConcurrentDictionary<string, OutgoingTransfer> _outgoingTransfers = new();
    private readonly ConcurrentDictionary<string, IncomingTransfer> _incomingTransfers = new();
    private readonly ConcurrentDictionary<string, RoomFileUploadState> _roomUploadStates = new();
    private readonly ConcurrentDictionary<string, RoomFileDownloadState> _roomDownloadStates = new();
    private readonly string _downloadRoot;
    private readonly string _roomDownloadRoot;
    private const int FileChunkSize = 64 * 1024;

    public event MessageReceivedHandler? MessageReceived;
    public event PrivateMessageReceivedHandler? PrivateMessageReceived;
    public event UsersUpdatedHandler? UsersUpdated;
    public event UserTypingHandler? UserTyping;
    public event ConnectionStateHandler? ConnectionLost;
    public event ConnectionStateHandler? ConnectionRestored;
    public event FileTransferUpdatedHandler? FileTransferUpdated;
    public event RoomFilesUpdatedHandler? RoomFilesUpdated;

    public string? CurrentUsername { get; private set; }
    public bool IsConnected => _isConnected;

    public ChatService(MessageCache messageCache)
    {
        _messageCache = messageCache;
        _downloadRoot = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "ChatTCP");
        _roomDownloadRoot = Path.Combine(_downloadRoot, "RoomFiles");
    }

    private sealed class RoomFileUploadState
    {
        public string TransferId { get; init; } = string.Empty;
        public string RoomId { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public string FilePath { get; init; } = string.Empty;
        public long TotalBytes { get; init; }
        public CancellationTokenSource Cts { get; } = new();
        public long SentBytes { get; set; }
    }

    private sealed class RoomFileDownloadState
    {
        public string FileId { get; init; } = string.Empty;
        public string RoomId { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public string TempPath { get; init; } = string.Empty;
        public string FinalPath { get; init; } = string.Empty;
        public long TotalBytes { get; init; }
        public long ReceivedBytes { get; set; }
        public FileStream? Stream { get; set; }
        public object SyncRoot { get; } = new();
        public FileTransferStatus Status { get; set; } = FileTransferStatus.Pending;
    }

    private sealed class OutgoingTransfer
    {
        public string TransferId { get; init; } = string.Empty;
        public string RoomId { get; init; } = string.Empty;
        public string Sender { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public string FilePath { get; init; } = string.Empty;
        public long TotalBytes { get; init; }
        public ConcurrentDictionary<string, OutgoingRecipientState> Recipients { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class OutgoingRecipientState
    {
        public string Recipient { get; init; } = string.Empty;
        public CancellationTokenSource Cts { get; init; } = new();
        public Task? Task { get; set; }
        public long SentBytes { get; set; }
    }

    private sealed class IncomingTransfer
    {
        public string TransferId { get; init; } = string.Empty;
        public string RoomId { get; init; } = string.Empty;
        public string Sender { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public long TotalBytes { get; init; }
        public string TempPath { get; init; } = string.Empty;
        public string FinalPath { get; init; } = string.Empty;
        public long ReceivedBytes { get; set; }
        public FileStream? Stream { get; set; }
        public object SyncRoot { get; } = new();
        public FileTransferStatus Status { get; set; } = FileTransferStatus.Pending;
    }

    public async Task ConnectAsync(string ip, int port, string username)
    {
        Console.WriteLine($"[CLIENT] connect requested: {ip}:{port} as {username}");

        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_isConnected)
            {
                Console.WriteLine("[CLIENT] connect ignored: already connected");
                return;
            }

            if (_isConnecting)
            {
                Console.WriteLine("[CLIENT] connect ignored: already connecting");
                return;
            }

            _isConnecting = true;
            _isIntentionallyDisconnected = false;
            _lastIp = ip;
            _lastPort = port;
            _lastUsername = username;
            CurrentUsername = username;

            await OpenConnectionLockedAsync().ConfigureAwait(false);
        }
        catch
        {
            if (!_isConnected)
            {
                ClearConnectionTarget();
                _isIntentionallyDisconnected = true;
            }

            throw;
        }
        finally
        {
            _isConnecting = false;
            _connectionLock.Release();
        }
    }

    public async Task JoinRoomAsync(string roomId, bool setActive = true)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_isConnected)
        {
            if (setActive) _activeRoomId = normalizedRoomId;
            return;
        }

        var sent = await TrySendPacketAsync(new Packet
        {
            Type = PacketType.RoomJoin,
            Data = JsonSerializer.SerializeToElement(new RoomJoinData { RoomId = normalizedRoomId })
        }).ConfigureAwait(false);

        if (sent && setActive) _activeRoomId = normalizedRoomId;
    }

    private async Task OpenConnectionLockedAsync()
    {
        CleanupConnectionLocked(invalidateConnection: true);

        TcpClient? client = null;
        NetworkStream? stream = null;
        StreamReader? reader = null;
        StreamWriter? writer = null;
        CancellationTokenSource? cts = null;

        try
        {
            client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(_lastIp!, _lastPort).ConfigureAwait(false);

            stream = client.GetStream();
            reader = new StreamReader(stream, Encoding.UTF8);
            writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
            cts = new CancellationTokenSource();

            await WritePacketAsync(writer, new Packet
            {
                Type = PacketType.Join,
                Data = JsonSerializer.SerializeToElement(new JoinData { Username = _lastUsername! })
            }).ConfigureAwait(false);

            await WaitForJoinAcceptedAsync(reader, cts.Token).ConfigureAwait(false);

            _client = client;
            _stream = stream;
            _reader = reader;
            _writer = writer;
            _cts = cts;
            _isConnected = true;

            ConnectionRestored?.Invoke();
            _ = ResumePendingIncomingTransfersAsync();

            var connectionId = ++_connectionId;
            Console.WriteLine($"[CLIENT] connect success: connection #{connectionId}");

            var receiveReader = reader;
            var connectionToken = cts.Token;
            _receiveTask = Task.Run(() => ReceiveLoopAsync(connectionId, receiveReader, connectionToken));
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(connectionId, connectionToken));

            client = null;
            stream = null;
            reader = null;
            writer = null;
            cts = null;
        }
        catch
        {
            SafeDispose(cts);
            SafeDispose(reader);
            SafeDispose(writer);
            SafeDispose(stream);
            SafeDispose(client);
            _isConnected = false;
            throw;
        }
    }

    private async Task WaitForJoinAcceptedAsync(StreamReader reader, CancellationToken token)
    {
        using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(token);
        handshakeCts.CancelAfter(TimeSpan.FromSeconds(5));

        try
        {
            while (true)
            {
                var line = await reader.ReadLineAsync(handshakeCts.Token).ConfigureAwait(false);
                if (line == null)
                {
                    throw new IOException("Server closed the connection during join.");
                }

                var packet = JsonSerializer.Deserialize<Packet>(line);
                if (packet == null)
                {
                    continue;
                }

                if (packet.Type == PacketType.ConnectionRejected)
                {
                    var rejectedData = packet.Data.Deserialize<SystemMessageData>();
                    _isIntentionallyDisconnected = true;
                    throw new InvalidOperationException(rejectedData?.Message ?? "Connection rejected by server.");
                }

                ParseLine(line);

                if (IsJoinAccepted(packet))
                {
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested && handshakeCts.IsCancellationRequested)
        {
            throw new TimeoutException("Timed out waiting for join response from server.");
        }
    }

    private bool IsJoinAccepted(Packet packet)
    {
        if (packet.Type != PacketType.UserListUpdate || string.IsNullOrWhiteSpace(_lastUsername))
        {
            return false;
        }

        var userData = packet.Data.Deserialize<UserListUpdateData>();
        return userData?.Users.Exists(user => string.Equals(user, _lastUsername, StringComparison.OrdinalIgnoreCase)) == true;
    }

    public async Task<string?> SendMessageAsync(string message)
    {
        var messageId = Guid.NewGuid().ToString("N");
        var sent = await TrySendPacketAsync(new Packet
        {
            Type = PacketType.ChatMessage,
            Data = JsonSerializer.SerializeToElement(new ChatMessageData
            {
                MessageId = messageId,
                RoomId = _activeRoomId,
                Username = _lastUsername!,
                Content = message
            })
        }).ConfigureAwait(false);

        return sent ? messageId : null;
    }

    public async Task<string?> SendFileAsync(string filePath, string roomId)
    {
        if (!_isConnected || string.IsNullOrWhiteSpace(_lastUsername)) return null;
        if (!File.Exists(filePath)) return null;

        var transferId = Guid.NewGuid().ToString("N");
        var fileInfo = new FileInfo(filePath);
        var normalizedRoomId = NormalizeRoomId(roomId);

        var uploadState = new RoomFileUploadState
        {
            TransferId = transferId,
            RoomId = normalizedRoomId,
            FileName = fileInfo.Name,
            FilePath = filePath,
            TotalBytes = fileInfo.Length
        };

        _roomUploadStates[transferId] = uploadState;

        RaiseTransferUpdate(new FileTransferUpdate
        {
            TransferKey = BuildRoomUploadKey(transferId),
            TransferId = transferId,
            RoomId = normalizedRoomId,
            Peer = "Room",
            FileName = fileInfo.Name,
            TotalBytes = fileInfo.Length,
            TransferredBytes = 0,
            Direction = FileTransferDirection.Outgoing,
            Status = FileTransferStatus.Pending
        });

        var sent = await TrySendPacketAsync(new Packet
        {
            Type = PacketType.RoomFileUploadRequest,
            Data = JsonSerializer.SerializeToElement(new RoomFileUploadRequestData
            {
                TransferId = transferId,
                RoomId = normalizedRoomId,
                Sender = _lastUsername,
                FileName = fileInfo.Name,
                FileSize = fileInfo.Length
            })
        }).ConfigureAwait(false);

        return sent ? transferId : null;
    }

    public async Task DownloadRoomFileAsync(string roomId, string fileId, string fileName)
    {
        if (!_isConnected) return;

        var normalizedRoomId = NormalizeRoomId(roomId);
        var downloadFolder = Path.Combine(_roomDownloadRoot, normalizedRoomId);
        Directory.CreateDirectory(downloadFolder);

        var safeFileName = Path.GetFileName(fileName);
        var tempPath = Path.Combine(downloadFolder, $"{fileId}_{safeFileName}.part");
        var finalPath = Path.Combine(downloadFolder, $"{fileId}_{safeFileName}");

        long existingBytes = 0;
        if (File.Exists(tempPath))
        {
            existingBytes = new FileInfo(tempPath).Length;
        }

        var downloadState = _roomDownloadStates.GetOrAdd(fileId, _ => new RoomFileDownloadState
        {
            FileId = fileId,
            RoomId = normalizedRoomId,
            FileName = safeFileName,
            TempPath = tempPath,
            FinalPath = finalPath,
            TotalBytes = 0,
            ReceivedBytes = existingBytes,
            Status = FileTransferStatus.Pending
        });

        RaiseTransferUpdate(new FileTransferUpdate
        {
            TransferKey = BuildRoomDownloadKey(fileId),
            TransferId = fileId,
            RoomId = normalizedRoomId,
            Peer = "Room",
            FileName = safeFileName,
            TotalBytes = downloadState.TotalBytes,
            TransferredBytes = existingBytes,
            Direction = FileTransferDirection.Incoming,
            Status = FileTransferStatus.Pending
        });

        await TrySendPacketAsync(new Packet
        {
            Type = PacketType.RoomFileDownloadRequest,
            Data = JsonSerializer.SerializeToElement(new RoomFileDownloadRequestData
            {
                FileId = fileId,
                RoomId = normalizedRoomId,
                Offset = existingBytes
            })
        }).ConfigureAwait(false);
    }

    public async Task CancelFileTransferAsync(string transferKey)
    {
        if (string.IsNullOrWhiteSpace(transferKey)) return;

        if (TryParseRoomUploadKey(transferKey, out var uploadId) && _roomUploadStates.TryGetValue(uploadId, out var uploadState))
        {
            uploadState.Cts.Cancel();
            RaiseTransferUpdate(new FileTransferUpdate
            {
                TransferKey = BuildRoomUploadKey(uploadId),
                TransferId = uploadId,
                RoomId = uploadState.RoomId,
                Peer = "Room",
                FileName = uploadState.FileName,
                TotalBytes = uploadState.TotalBytes,
                TransferredBytes = uploadState.SentBytes,
                Direction = FileTransferDirection.Outgoing,
                Status = FileTransferStatus.Canceled
            });

            _roomUploadStates.TryRemove(uploadId, out _);
            return;
        }

        if (TryParseRoomDownloadKey(transferKey, out var downloadId) && _roomDownloadStates.TryGetValue(downloadId, out var downloadState))
        {
            lock (downloadState.SyncRoot)
            {
                downloadState.Status = FileTransferStatus.Canceled;
                SafeDispose(downloadState.Stream);
                downloadState.Stream = null;
            }

            RaiseTransferUpdate(new FileTransferUpdate
            {
                TransferKey = BuildRoomDownloadKey(downloadId),
                TransferId = downloadId,
                RoomId = downloadState.RoomId,
                Peer = "Room",
                FileName = downloadState.FileName,
                TotalBytes = downloadState.TotalBytes,
                TransferredBytes = downloadState.ReceivedBytes,
                Direction = FileTransferDirection.Incoming,
                Status = FileTransferStatus.Canceled
            });

            _roomDownloadStates.TryRemove(downloadId, out _);
            return;
        }

        if (TryParseTransferKey(transferKey, out var transferId, out var recipient))
        {
            if (_outgoingTransfers.TryGetValue(transferId, out var outgoing))
            {
                if (!string.IsNullOrWhiteSpace(recipient) && outgoing.Recipients.TryGetValue(recipient, out var state))
                {
                    state.Cts.Cancel();
                    RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, state.SentBytes, FileTransferStatus.Canceled));

                    await TrySendPacketAsync(new Packet
                    {
                        Type = PacketType.FileTransferCancel,
                        Data = JsonSerializer.SerializeToElement(new FileTransferCancelData
                        {
                            TransferId = transferId,
                            RoomId = outgoing.RoomId,
                            Sender = outgoing.Sender,
                            Recipient = recipient,
                            Reason = "Canceled by sender"
                        })
                    }).ConfigureAwait(false);
                }
            }

            return;
        }

        if (_incomingTransfers.TryGetValue(transferKey, out var incoming))
        {
            lock (incoming.SyncRoot)
            {
                incoming.Status = FileTransferStatus.Canceled;
                SafeDispose(incoming.Stream);
                incoming.Stream = null;
            }

            RaiseTransferUpdate(BuildIncomingUpdate(incoming, incoming.ReceivedBytes, FileTransferStatus.Canceled));

            await TrySendPacketAsync(new Packet
            {
                Type = PacketType.FileTransferCancel,
                Data = JsonSerializer.SerializeToElement(new FileTransferCancelData
                {
                    TransferId = incoming.TransferId,
                    RoomId = incoming.RoomId,
                    Sender = incoming.Sender,
                    Recipient = _lastUsername ?? string.Empty,
                    Reason = "Canceled by receiver"
                })
            }).ConfigureAwait(false);
        }
    }

    public async Task SendTypingAsync(bool isTyping)
    {
        await TrySendPacketAsync(new Packet
        {
            Type = PacketType.Typing,
            Data = JsonSerializer.SerializeToElement(new TypingData
            {
                Username = _lastUsername!,
                IsTyping = isTyping
            })
        }).ConfigureAwait(false);
    }

    private async Task<bool> TrySendPacketAsync(Packet packet, CancellationToken token = default)
    {
        var writer = _writer;
        var connectionId = _connectionId;
        if (writer == null || !_isConnected || token.IsCancellationRequested) return false;

        var lockTaken = false;
        try
        {
            await _writeLock.WaitAsync(token).ConfigureAwait(false);
            lockTaken = true;

            if (writer != _writer || connectionId != _connectionId || !_isConnected)
            {
                return false;
            }

            await WritePacketAsync(writer, packet).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (ObjectDisposedException)
        {
            await HandleDisconnectAsync(connectionId, "send failed: socket disposed").ConfigureAwait(false);
            return false;
        }
        catch (Exception ex)
        {
            await HandleDisconnectAsync(connectionId, $"send failed: {ex.Message}").ConfigureAwait(false);
            return false;
        }
        finally
        {
            if (lockTaken) _writeLock.Release();
        }
    }

    private static async Task WritePacketAsync(StreamWriter writer, Packet packet)
    {
        string json = JsonSerializer.Serialize(packet);
        await writer.WriteLineAsync(json).ConfigureAwait(false);
    }

    private async Task ReceiveLoopAsync(int connectionId, StreamReader reader, CancellationToken token)
    {
        Console.WriteLine($"[CLIENT] receive loop started: connection #{connectionId}");
        var disconnectReason = "remote closed connection";

        try
        {
            while (!token.IsCancellationRequested && _isConnected && connectionId == _connectionId)
            {
                string? line = await reader.ReadLineAsync(token).ConfigureAwait(false);
                if (line == null)
                {
                    break;
                }

                ParseLine(line);
            }
        }
        catch (OperationCanceledException)
        {
            disconnectReason = "cancelled";
        }
        catch (ObjectDisposedException)
        {
            disconnectReason = "socket disposed";
        }
        catch (IOException ex)
        {
            disconnectReason = $"io error: {ex.Message}";
        }
        catch (Exception ex)
        {
            disconnectReason = $"receive failed: {ex.Message}";
        }
        finally
        {
            Console.WriteLine($"[CLIENT] receive loop ended: connection #{connectionId} ({disconnectReason})");

            if (!token.IsCancellationRequested)
            {
                await HandleDisconnectAsync(connectionId, disconnectReason).ConfigureAwait(false);
            }
        }
    }

    private async Task HeartbeatLoopAsync(int connectionId, CancellationToken token)
    {
        try
        {
            while (!token.IsCancellationRequested && _isConnected && connectionId == _connectionId)
            {
                bool sent = await TrySendPacketAsync(new Packet
                {
                    Type = PacketType.Heartbeat,
                    Data = JsonSerializer.SerializeToElement(new HeartbeatData())
                }, token).ConfigureAwait(false);
                if (!sent && !token.IsCancellationRequested)
                {
                    await HandleDisconnectAsync(connectionId, "heartbeat send failed").ConfigureAwait(false);
                    break;
                }

                await Task.Delay(5000, token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            await HandleDisconnectAsync(connectionId, $"heartbeat failed: {ex.Message}").ConfigureAwait(false);
        }
    }

    private void ParseLine(string line)
    {
        try
        {
            var packet = JsonSerializer.Deserialize<Packet>(line);
            if (packet == null) return;

            Console.WriteLine($"[CLIENT] packet received: {packet.Type}");

            switch (packet.Type)
            {
                case PacketType.ChatMessage:
                    var chatData = packet.Data.Deserialize<ChatMessageData>();
                    if (chatData != null)
                    {
                        Console.WriteLine($"[CLIENT] chat packet: room={NormalizeRoomId(chatData.RoomId)}, from={chatData.Username}, id={chatData.MessageId}");
                        _ = _messageCache.SaveMessageAsync(chatData);
                        DateTime time;
                        if (!DateTime.TryParse(chatData.Timestamp, out time)) time = DateTime.Now;
                        MessageReceived?.Invoke(chatData.Username, time.ToString("HH:mm"), chatData.Content, chatData.MessageId, NormalizeRoomId(chatData.RoomId));
                    }
                    break;
                case PacketType.PrivateMessage:
                    var privateData = packet.Data.Deserialize<PrivateMessageData>();
                    if (privateData != null)
                    {
                        _ = _messageCache.SavePrivateMessageAsync(privateData);
                        DateTime privateTime;
                        if (!DateTime.TryParse(privateData.Timestamp, out privateTime)) privateTime = DateTime.Now;
                        PrivateMessageReceived?.Invoke(privateData.Sender, privateTime.ToString("HH:mm"), privateData.Content, privateData.MessageId);
                    }
                    break;
                case PacketType.UserListUpdate:
                    var userData = packet.Data.Deserialize<UserListUpdateData>();
                    if (userData != null) UsersUpdated?.Invoke(userData.Users.ToArray());
                    break;
                case PacketType.Typing:
                    var typingData = packet.Data.Deserialize<TypingData>();
                    if (typingData != null) UserTyping?.Invoke(typingData.Username, typingData.IsTyping);
                    break;
                case PacketType.SystemMessage:
                    var systemData = packet.Data.Deserialize<SystemMessageData>();
                    if (systemData != null) MessageReceived?.Invoke("SYSTEM", DateTime.Now.ToString("HH:mm"), systemData.Message, "", _activeRoomId);
                    break;
                case PacketType.ConnectionRejected:
                    var rejectedData = packet.Data.Deserialize<SystemMessageData>();
                    Console.WriteLine($"[CLIENT] connection rejected: {rejectedData?.Message ?? "no reason"}");
                    _isIntentionallyDisconnected = true;
                    if (rejectedData != null) MessageReceived?.Invoke("SYSTEM", DateTime.Now.ToString("HH:mm"), rejectedData.Message, "", _activeRoomId);
                    break;
                case PacketType.Heartbeat:
                    break;
                case PacketType.FileTransferRequest:
                    var requestData = packet.Data.Deserialize<FileTransferRequestData>();
                    if (requestData != null) _ = HandleFileTransferRequestAsync(requestData);
                    break;
                case PacketType.FileTransferResume:
                    var resumeData = packet.Data.Deserialize<FileTransferResumeData>();
                    if (resumeData != null) _ = HandleFileTransferResumeAsync(resumeData);
                    break;
                case PacketType.FileTransferChunk:
                    var chunkData = packet.Data.Deserialize<FileTransferChunkData>();
                    if (chunkData != null) _ = HandleFileTransferChunkAsync(chunkData);
                    break;
                case PacketType.FileTransferCancel:
                    var cancelData = packet.Data.Deserialize<FileTransferCancelData>();
                    if (cancelData != null) _ = HandleFileTransferCancelAsync(cancelData);
                    break;
                case PacketType.RoomFileList:
                    var roomFileList = packet.Data.Deserialize<RoomFileListData>();
                    if (roomFileList != null) RoomFilesUpdated?.Invoke(NormalizeRoomId(roomFileList.RoomId), roomFileList.Files);
                    break;
                case PacketType.RoomFileUploadResume:
                    var uploadResume = packet.Data.Deserialize<RoomFileUploadResumeData>();
                    if (uploadResume != null) _ = HandleRoomFileUploadResumeAsync(uploadResume);
                    break;
                case PacketType.RoomFileDownloadChunk:
                    var downloadChunk = packet.Data.Deserialize<RoomFileDownloadChunkData>();
                    if (downloadChunk != null) _ = HandleRoomFileDownloadChunkAsync(downloadChunk);
                    break;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CLIENT] packet parse/dispatch failed: {ex.Message}");
        }
    }

    private async Task HandleFileTransferRequestAsync(FileTransferRequestData request)
    {
        if (string.IsNullOrWhiteSpace(request.Sender) || request.Sender.Equals(_lastUsername, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        Directory.CreateDirectory(_downloadRoot);

        var transfer = _incomingTransfers.GetOrAdd(request.TransferId, _ =>
        {
            var safeFileName = Path.GetFileName(request.FileName);
            var tempName = $"{request.TransferId}_{safeFileName}.part";
            var finalName = $"{request.TransferId}_{safeFileName}";
            var tempPath = Path.Combine(_downloadRoot, tempName);
            var finalPath = Path.Combine(_downloadRoot, finalName);
            long existingBytes = 0;

            if (File.Exists(tempPath))
            {
                existingBytes = new FileInfo(tempPath).Length;
            }

            var incoming = new IncomingTransfer
            {
                TransferId = request.TransferId,
                RoomId = NormalizeRoomId(request.RoomId),
                Sender = request.Sender,
                FileName = safeFileName,
                TotalBytes = request.FileSize,
                TempPath = tempPath,
                FinalPath = finalPath,
                ReceivedBytes = existingBytes,
                Status = FileTransferStatus.Pending
            };

            RaiseTransferUpdate(BuildIncomingUpdate(incoming, existingBytes, FileTransferStatus.Pending));
            return incoming;
        });

        await SendResumeAsync(transfer).ConfigureAwait(false);
    }

    private Task HandleFileTransferResumeAsync(FileTransferResumeData resume)
    {
        if (!_outgoingTransfers.TryGetValue(resume.TransferId, out var outgoing)) return Task.CompletedTask;
        if (string.IsNullOrWhiteSpace(resume.Recipient)) return Task.CompletedTask;

        var recipientState = outgoing.Recipients.AddOrUpdate(
            resume.Recipient,
            _ => new OutgoingRecipientState { Recipient = resume.Recipient, SentBytes = resume.ReceivedBytes },
            (_, existing) =>
            {
                existing.Cts.Cancel();
                existing.Cts.Dispose();
                return new OutgoingRecipientState { Recipient = resume.Recipient, SentBytes = resume.ReceivedBytes };
            });

        recipientState.Task = Task.Run(() => SendChunksToRecipientAsync(outgoing, recipientState, resume.ReceivedBytes));
        RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, resume.Recipient, resume.ReceivedBytes, FileTransferStatus.InProgress));

        return Task.CompletedTask;
    }

    private Task HandleFileTransferChunkAsync(FileTransferChunkData chunk)
    {
        if (!_incomingTransfers.TryGetValue(chunk.TransferId, out var incoming))
        {
            return Task.CompletedTask;
        }

        if (!string.Equals(incoming.Sender, chunk.Sender, StringComparison.OrdinalIgnoreCase))
        {
            return Task.CompletedTask;
        }

        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(chunk.DataBase64);
        }
        catch
        {
            return Task.CompletedTask;
        }

        bool completed = false;
        long receivedBytes;

        lock (incoming.SyncRoot)
        {
            if (incoming.Status == FileTransferStatus.Canceled || incoming.Status == FileTransferStatus.Failed)
            {
                return Task.CompletedTask;
            }

            if (chunk.Offset < incoming.ReceivedBytes)
            {
                return Task.CompletedTask;
            }

            if (chunk.Offset > incoming.ReceivedBytes)
            {
                _ = SendResumeAsync(incoming);
                return Task.CompletedTask;
            }

            if (incoming.Stream == null)
            {
                incoming.Stream = new FileStream(incoming.TempPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                incoming.Stream.Seek(incoming.ReceivedBytes, SeekOrigin.Begin);
            }

            incoming.Stream.Write(payload, 0, payload.Length);
            incoming.ReceivedBytes += payload.Length;
            incoming.Status = FileTransferStatus.InProgress;
            receivedBytes = incoming.ReceivedBytes;

            if (chunk.IsLast || incoming.ReceivedBytes >= incoming.TotalBytes)
            {
                completed = true;
            }
        }

        RaiseTransferUpdate(BuildIncomingUpdate(incoming, receivedBytes, FileTransferStatus.InProgress));

        if (completed)
        {
            CompleteIncomingTransfer(incoming);
        }

        return Task.CompletedTask;
    }

    private Task HandleFileTransferCancelAsync(FileTransferCancelData cancel)
    {
        if (!string.IsNullOrWhiteSpace(cancel.Recipient) &&
            string.Equals(cancel.Recipient, _lastUsername, StringComparison.OrdinalIgnoreCase))
        {
            if (_incomingTransfers.TryGetValue(cancel.TransferId, out var incoming))
            {
                lock (incoming.SyncRoot)
                {
                    incoming.Status = FileTransferStatus.Canceled;
                    SafeDispose(incoming.Stream);
                    incoming.Stream = null;
                }

                RaiseTransferUpdate(BuildIncomingUpdate(incoming, incoming.ReceivedBytes, FileTransferStatus.Canceled));
            }

            return Task.CompletedTask;
        }

        if (string.IsNullOrWhiteSpace(cancel.Recipient) && _incomingTransfers.TryGetValue(cancel.TransferId, out var roomIncoming))
        {
            if (string.Equals(roomIncoming.Sender, cancel.Sender, StringComparison.OrdinalIgnoreCase))
            {
                lock (roomIncoming.SyncRoot)
                {
                    roomIncoming.Status = FileTransferStatus.Canceled;
                    SafeDispose(roomIncoming.Stream);
                    roomIncoming.Stream = null;
                }

                RaiseTransferUpdate(BuildIncomingUpdate(roomIncoming, roomIncoming.ReceivedBytes, FileTransferStatus.Canceled));
                return Task.CompletedTask;
            }
        }

        if (_outgoingTransfers.TryGetValue(cancel.TransferId, out var outgoing))
        {
            var recipient = cancel.Recipient;
            if (!string.IsNullOrWhiteSpace(recipient) && outgoing.Recipients.TryGetValue(recipient, out var state))
            {
                state.Cts.Cancel();
                RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, state.SentBytes, FileTransferStatus.Canceled));
            }
        }

        return Task.CompletedTask;
    }

    private async Task SendResumeAsync(IncomingTransfer incoming)
    {
        if (string.IsNullOrWhiteSpace(_lastUsername)) return;

        await TrySendPacketAsync(new Packet
        {
            Type = PacketType.FileTransferResume,
            Data = JsonSerializer.SerializeToElement(new FileTransferResumeData
            {
                TransferId = incoming.TransferId,
                RoomId = incoming.RoomId,
                Sender = incoming.Sender,
                Recipient = _lastUsername,
                ReceivedBytes = incoming.ReceivedBytes
            })
        }).ConfigureAwait(false);
    }

    private async Task ResumePendingIncomingTransfersAsync()
    {
        foreach (var transfer in _incomingTransfers.Values)
        {
            if (transfer.Status == FileTransferStatus.Completed || transfer.Status == FileTransferStatus.Canceled)
            {
                continue;
            }

            await SendResumeAsync(transfer).ConfigureAwait(false);
        }
    }

    private Task HandleRoomFileUploadResumeAsync(RoomFileUploadResumeData resume)
    {
        if (!_roomUploadStates.TryGetValue(resume.TransferId, out var uploadState)) return Task.CompletedTask;

        uploadState.SentBytes = resume.ReceivedBytes;
        RaiseTransferUpdate(new FileTransferUpdate
        {
            TransferKey = BuildRoomUploadKey(uploadState.TransferId),
            TransferId = uploadState.TransferId,
            RoomId = uploadState.RoomId,
            Peer = "Room",
            FileName = uploadState.FileName,
            TotalBytes = uploadState.TotalBytes,
            TransferredBytes = resume.ReceivedBytes,
            Direction = FileTransferDirection.Outgoing,
            Status = FileTransferStatus.InProgress
        });

        _ = Task.Run(() => SendRoomUploadChunksAsync(uploadState, resume.ReceivedBytes));

        return Task.CompletedTask;
    }

    private async Task SendRoomUploadChunksAsync(RoomFileUploadState uploadState, long offset)
    {
        try
        {
            using var stream = new FileStream(uploadState.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Seek(offset, SeekOrigin.Begin);
            var buffer = new byte[FileChunkSize];
            long sentBytes = offset;

            while (sentBytes < uploadState.TotalBytes && !uploadState.Cts.IsCancellationRequested)
            {
                int read = await stream.ReadAsync(buffer, 0, buffer.Length, uploadState.Cts.Token).ConfigureAwait(false);
                if (read <= 0) break;

                var payload = Convert.ToBase64String(buffer, 0, read);
                var isLast = sentBytes + read >= uploadState.TotalBytes;

                var sent = await TrySendPacketAsync(new Packet
                {
                    Type = PacketType.RoomFileUploadChunk,
                    Data = JsonSerializer.SerializeToElement(new RoomFileUploadChunkData
                    {
                        TransferId = uploadState.TransferId,
                        RoomId = uploadState.RoomId,
                        Offset = sentBytes,
                        DataBase64 = payload,
                        IsLast = isLast
                    })
                }, uploadState.Cts.Token).ConfigureAwait(false);

                if (!sent) break;

                sentBytes += read;
                uploadState.SentBytes = sentBytes;

                RaiseTransferUpdate(new FileTransferUpdate
                {
                    TransferKey = BuildRoomUploadKey(uploadState.TransferId),
                    TransferId = uploadState.TransferId,
                    RoomId = uploadState.RoomId,
                    Peer = "Room",
                    FileName = uploadState.FileName,
                    TotalBytes = uploadState.TotalBytes,
                    TransferredBytes = sentBytes,
                    Direction = FileTransferDirection.Outgoing,
                    Status = FileTransferStatus.InProgress
                });
            }

            if (!uploadState.Cts.IsCancellationRequested && sentBytes >= uploadState.TotalBytes)
            {
                RaiseTransferUpdate(new FileTransferUpdate
                {
                    TransferKey = BuildRoomUploadKey(uploadState.TransferId),
                    TransferId = uploadState.TransferId,
                    RoomId = uploadState.RoomId,
                    Peer = "Room",
                    FileName = uploadState.FileName,
                    TotalBytes = uploadState.TotalBytes,
                    TransferredBytes = sentBytes,
                    Direction = FileTransferDirection.Outgoing,
                    Status = FileTransferStatus.Completed
                });
            }
        }
        catch (OperationCanceledException)
        {
            RaiseTransferUpdate(new FileTransferUpdate
            {
                TransferKey = BuildRoomUploadKey(uploadState.TransferId),
                TransferId = uploadState.TransferId,
                RoomId = uploadState.RoomId,
                Peer = "Room",
                FileName = uploadState.FileName,
                TotalBytes = uploadState.TotalBytes,
                TransferredBytes = uploadState.SentBytes,
                Direction = FileTransferDirection.Outgoing,
                Status = FileTransferStatus.Canceled
            });
        }
        catch (Exception ex)
        {
            RaiseTransferUpdate(new FileTransferUpdate
            {
                TransferKey = BuildRoomUploadKey(uploadState.TransferId),
                TransferId = uploadState.TransferId,
                RoomId = uploadState.RoomId,
                Peer = "Room",
                FileName = uploadState.FileName,
                TotalBytes = uploadState.TotalBytes,
                TransferredBytes = uploadState.SentBytes,
                Direction = FileTransferDirection.Outgoing,
                Status = FileTransferStatus.Failed,
                Error = ex.Message
            });
        }
    }

    private Task HandleRoomFileDownloadChunkAsync(RoomFileDownloadChunkData chunk)
    {
        var descriptor = chunk.Descriptor;
        var fileId = chunk.FileId;
        var roomId = NormalizeRoomId(chunk.RoomId);
        var downloadFolder = Path.Combine(_roomDownloadRoot, roomId);
        Directory.CreateDirectory(downloadFolder);

        var safeFileName = Path.GetFileName(descriptor.FileName);
        var tempPath = Path.Combine(downloadFolder, $"{fileId}_{safeFileName}.part");
        var finalPath = Path.Combine(downloadFolder, $"{fileId}_{safeFileName}");

        var state = _roomDownloadStates.GetOrAdd(fileId, _ => new RoomFileDownloadState
        {
            FileId = fileId,
            RoomId = roomId,
            FileName = safeFileName,
            TempPath = tempPath,
            FinalPath = finalPath,
            TotalBytes = descriptor.FileSize,
            ReceivedBytes = 0,
            Status = FileTransferStatus.Pending
        });

        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(chunk.DataBase64);
        }
        catch
        {
            return Task.CompletedTask;
        }

        bool completed = false;
        long receivedBytes;

        lock (state.SyncRoot)
        {
            if (state.Status == FileTransferStatus.Canceled || state.Status == FileTransferStatus.Failed)
            {
                return Task.CompletedTask;
            }

            if (chunk.Offset < state.ReceivedBytes)
            {
                return Task.CompletedTask;
            }

            if (chunk.Offset > state.ReceivedBytes)
            {
                return Task.CompletedTask;
            }

            if (state.Stream == null)
            {
                state.Stream = new FileStream(state.TempPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
                state.Stream.Seek(state.ReceivedBytes, SeekOrigin.Begin);
            }

            state.Stream.Write(payload, 0, payload.Length);
            state.ReceivedBytes += payload.Length;
            state.Status = FileTransferStatus.InProgress;
            receivedBytes = state.ReceivedBytes;

            if (chunk.IsLast || state.ReceivedBytes >= descriptor.FileSize)
            {
                completed = true;
            }
        }

        RaiseTransferUpdate(new FileTransferUpdate
        {
            TransferKey = BuildRoomDownloadKey(fileId),
            TransferId = fileId,
            RoomId = roomId,
            Peer = "Room",
            FileName = safeFileName,
            TotalBytes = descriptor.FileSize,
            TransferredBytes = receivedBytes,
            Direction = FileTransferDirection.Incoming,
            Status = FileTransferStatus.InProgress
        });

        if (completed)
        {
            CompleteRoomDownload(state, descriptor.FileSize);
        }

        return Task.CompletedTask;
    }

    private void CompleteRoomDownload(RoomFileDownloadState state, long totalBytes)
    {
        lock (state.SyncRoot)
        {
            if (state.Status == FileTransferStatus.Completed) return;
            state.Status = FileTransferStatus.Completed;
            SafeDispose(state.Stream);
            state.Stream = null;
        }

        try
        {
            File.Move(state.TempPath, EnsureUniquePath(state.FinalPath), overwrite: true);
        }
        catch
        {
        }

        RaiseTransferUpdate(new FileTransferUpdate
        {
            TransferKey = BuildRoomDownloadKey(state.FileId),
            TransferId = state.FileId,
            RoomId = state.RoomId,
            Peer = "Room",
            FileName = state.FileName,
            TotalBytes = totalBytes,
            TransferredBytes = state.ReceivedBytes,
            Direction = FileTransferDirection.Incoming,
            Status = FileTransferStatus.Completed
        });
    }

    private static string BuildRoomUploadKey(string transferId)
    {
        return $"room-upload:{transferId}";
    }

    private static string BuildRoomDownloadKey(string fileId)
    {
        return $"room-download:{fileId}";
    }

    private static bool TryParseRoomUploadKey(string transferKey, out string transferId)
    {
        transferId = string.Empty;
        if (!transferKey.StartsWith("room-upload:", StringComparison.OrdinalIgnoreCase)) return false;
        transferId = transferKey.Substring("room-upload:".Length);
        return !string.IsNullOrWhiteSpace(transferId);
    }

    private static bool TryParseRoomDownloadKey(string transferKey, out string fileId)
    {
        fileId = string.Empty;
        if (!transferKey.StartsWith("room-download:", StringComparison.OrdinalIgnoreCase)) return false;
        fileId = transferKey.Substring("room-download:".Length);
        return !string.IsNullOrWhiteSpace(fileId);
    }

    private async Task SendChunksToRecipientAsync(OutgoingTransfer outgoing, OutgoingRecipientState recipientState, long offset)
    {
        var transferId = outgoing.TransferId;
        var recipient = recipientState.Recipient;

        try
        {
            using var stream = new FileStream(outgoing.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
            stream.Seek(offset, SeekOrigin.Begin);
            var buffer = new byte[FileChunkSize];
            long sentBytes = offset;

            while (sentBytes < outgoing.TotalBytes && !recipientState.Cts.IsCancellationRequested)
            {
                int read = await stream.ReadAsync(buffer, 0, buffer.Length, recipientState.Cts.Token).ConfigureAwait(false);
                if (read <= 0) break;

                var payload = Convert.ToBase64String(buffer, 0, read);
                var isLast = sentBytes + read >= outgoing.TotalBytes;

                var sent = await TrySendPacketAsync(new Packet
                {
                    Type = PacketType.FileTransferChunk,
                    Data = JsonSerializer.SerializeToElement(new FileTransferChunkData
                    {
                        TransferId = transferId,
                        RoomId = outgoing.RoomId,
                        Sender = outgoing.Sender,
                        Recipient = recipient,
                        Offset = sentBytes,
                        DataBase64 = payload,
                        IsLast = isLast
                    })
                }, recipientState.Cts.Token).ConfigureAwait(false);

                if (!sent) break;

                sentBytes += read;
                recipientState.SentBytes = sentBytes;
                RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, sentBytes, FileTransferStatus.InProgress));
            }

            if (!recipientState.Cts.IsCancellationRequested && sentBytes >= outgoing.TotalBytes)
            {
                RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, sentBytes, FileTransferStatus.Completed));
            }
        }
        catch (OperationCanceledException)
        {
            RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, recipientState.SentBytes, FileTransferStatus.Canceled));
        }
        catch (Exception ex)
        {
            RaiseTransferUpdate(BuildOutgoingUpdate(outgoing, recipient, recipientState.SentBytes, FileTransferStatus.Failed, ex.Message));
        }
    }

    private void CompleteIncomingTransfer(IncomingTransfer incoming)
    {
        lock (incoming.SyncRoot)
        {
            if (incoming.Status == FileTransferStatus.Completed) return;
            incoming.Status = FileTransferStatus.Completed;
            SafeDispose(incoming.Stream);
            incoming.Stream = null;
        }

        var finalPath = EnsureUniquePath(incoming.FinalPath);
        try
        {
            File.Move(incoming.TempPath, finalPath, overwrite: true);
        }
        catch
        {
        }

        RaiseTransferUpdate(BuildIncomingUpdate(incoming, incoming.ReceivedBytes, FileTransferStatus.Completed));
    }

    private static string EnsureUniquePath(string path)
    {
        if (!File.Exists(path)) return path;

        var directory = Path.GetDirectoryName(path) ?? string.Empty;
        var fileName = Path.GetFileNameWithoutExtension(path);
        var ext = Path.GetExtension(path);
        var index = 1;

        while (true)
        {
            var candidate = Path.Combine(directory, $"{fileName}_{index}{ext}");
            if (!File.Exists(candidate)) return candidate;
            index++;
        }
    }

    private void RaiseTransferUpdate(FileTransferUpdate update)
    {
        FileTransferUpdated?.Invoke(update);
    }

    private FileTransferUpdate BuildOutgoingUpdate(OutgoingTransfer outgoing, string recipient, long sentBytes, FileTransferStatus status, string? error = null)
    {
        return new FileTransferUpdate
        {
            TransferKey = BuildTransferKey(outgoing.TransferId, recipient),
            TransferId = outgoing.TransferId,
            RoomId = outgoing.RoomId,
            Peer = recipient,
            FileName = outgoing.FileName,
            TotalBytes = outgoing.TotalBytes,
            TransferredBytes = sentBytes,
            Direction = FileTransferDirection.Outgoing,
            Status = status,
            Error = error
        };
    }

    private FileTransferUpdate BuildIncomingUpdate(IncomingTransfer incoming, long receivedBytes, FileTransferStatus status, string? error = null)
    {
        return new FileTransferUpdate
        {
            TransferKey = incoming.TransferId,
            TransferId = incoming.TransferId,
            RoomId = incoming.RoomId,
            Peer = incoming.Sender,
            FileName = incoming.FileName,
            TotalBytes = incoming.TotalBytes,
            TransferredBytes = receivedBytes,
            Direction = FileTransferDirection.Incoming,
            Status = status,
            Error = error
        };
    }

    private static string BuildTransferKey(string transferId, string recipient)
    {
        return $"{transferId}:{recipient}";
    }

    private static bool TryParseTransferKey(string transferKey, out string transferId, out string recipient)
    {
        transferId = string.Empty;
        recipient = string.Empty;
        if (string.IsNullOrWhiteSpace(transferKey)) return false;

        var parts = transferKey.Split(':');
        if (parts.Length != 2) return false;

        transferId = parts[0];
        recipient = parts[1];
        return !string.IsNullOrWhiteSpace(transferId) && !string.IsNullOrWhiteSpace(recipient);
    }

    private async Task HandleDisconnectAsync(int connectionId, string reason)
    {
        var shouldReconnect = false;

        await _connectionLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (connectionId != _connectionId || !_isConnected)
            {
                return;
            }

            _isConnected = false;
            Console.WriteLine($"[CLIENT] disconnected: connection #{connectionId} ({reason})");
            CleanupConnectionLocked(invalidateConnection: true);
            shouldReconnect = !_isIntentionallyDisconnected;
        }
        finally
        {
            _connectionLock.Release();
        }

        ConnectionLost?.Invoke();

        if (shouldReconnect)
        {
            StartReconnectLoop();
        }
    }

    private void StartReconnectLoop()
    {
        if (_isIntentionallyDisconnected) return;

        if (_isReconnectLoopRunning)
        {
            Console.WriteLine("[CLIENT] reconnect ignored: loop already running");
            return;
        }

        _isReconnectLoopRunning = true;
        _reconnectCts?.Cancel();
        SafeDispose(_reconnectCts);
        _reconnectCts = new CancellationTokenSource();
        _reconnectTask = Task.Run(() => ReconnectLoopAsync(_reconnectCts.Token));
    }

    private async Task ReconnectLoopAsync(CancellationToken token)
    {
        Console.WriteLine("[CLIENT] reconnect started");

        try
        {
            int retryCount = 0;
            while (!_isConnected && !_isIntentionallyDisconnected && !token.IsCancellationRequested)
            {
                retryCount++;
                try
                {
                    var delay = retryCount <= 5 ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(5);
                    await Task.Delay(delay, token).ConfigureAwait(false);

                    await _connectionLock.WaitAsync(token).ConfigureAwait(false);
                    try
                    {
                        if (_isConnected || _isIntentionallyDisconnected || token.IsCancellationRequested)
                        {
                            break;
                        }

                        if (_isConnecting)
                        {
                            continue;
                        }

                        _isConnecting = true;
                        await OpenConnectionLockedAsync().ConfigureAwait(false);
                    }
                    finally
                    {
                        _isConnecting = false;
                        _connectionLock.Release();
                    }

                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CLIENT] reconnect attempt {retryCount} failed: {ex.Message}");
                }
            }
        }
        finally
        {
            _isReconnectLoopRunning = false;
            Console.WriteLine("[CLIENT] reconnect stopped");
        }
    }

    public void Disconnect()
    {
        Console.WriteLine("[CLIENT] disconnect requested");
        _isIntentionallyDisconnected = true;
        _reconnectCts?.Cancel();

        _connectionLock.Wait();
        try
        {
            _isConnecting = false;
            _isConnected = false;
            CleanupConnectionLocked(invalidateConnection: true);
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private void CleanupConnectionLocked(bool invalidateConnection)
    {
        if (invalidateConnection)
        {
            _connectionId++;
        }

        try { _cts?.Cancel(); } catch { }

        SafeDispose(_reader);
        SafeDispose(_writer);
        SafeDispose(_stream);
        SafeDispose(_client);
        SafeDispose(_cts);

        _reader = null;
        _writer = null;
        _stream = null;
        _client = null;
        _cts = null;
        _receiveTask = null;
        _heartbeatTask = null;
    }

    private void ClearConnectionTarget()
    {
        CurrentUsername = null;
        _lastUsername = null;
        _lastIp = null;
        _lastPort = 0;
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

    private static string NormalizeRoomId(string? roomId)
    {
        return string.IsNullOrWhiteSpace(roomId) ? "General" : roomId.Trim();
    }
}
