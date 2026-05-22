using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using CommunityToolkit.Mvvm.Messaging;
using Microsoft.Win32;
using WpfChatClient.Models;
using WpfChatClient.Core.Interfaces;
using WpfChatClient.Infrastructure;
using WpfChatClient.Messages;
using WpfChatClient.Services;
using WpfChatClient.Core.Models;

namespace WpfChatClient.ViewModels;

public partial class ChatViewModel : ObservableObject, IDisposable, IRecipient<ConnectionSuccessMessage>
{
    private readonly IChatService _chatService;
    private readonly MessageCache _messageCache;
    private readonly IStickerService _stickerService;
    private bool _disposed;
    private DateTime _lastTypingSent = DateTime.MinValue;
    private readonly DispatcherTimer _typingClearTimer;
    private readonly HashSet<string> _typingUsers = new();
    private readonly Dictionary<string, User> _knownUsers = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, RoomItem> _roomLookup = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<ChatMessage>> _roomMessages = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, HashSet<string>> _roomMessageIds = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, List<RoomFileItem>> _roomFiles = new(StringComparer.OrdinalIgnoreCase);
    private bool _hasJoinedRooms;
    private bool _hasConnectedOnce;

    [ObservableProperty]
    private string _messageText = "";

    [ObservableProperty]
    private string _typingStatus = "";

    [ObservableProperty]
    private bool _isSomeoneTyping;

    [ObservableProperty]
    private string _onlineHeader = "ONLINE - 0";

    [ObservableProperty]
    private UserProfileCard? _selectedProfile;

    [ObservableProperty]
    private bool _isProfileCardOpen;

    [ObservableProperty]
    private RoomItem? _selectedRoom;

    [ObservableProperty]
    private string _currentRoomId = "General";

    [ObservableProperty]
    private bool _isConnected;

    [ObservableProperty]
    private bool _isStickerPickerOpen;

    public ObservableCollection<ChatMessage> Messages { get; } = new();
    public ObservableCollection<User> Users { get; } = new();
    public ObservableCollection<ToastNotification> Toasts { get; } = new();
    public ObservableCollection<RoomItem> Rooms { get; } = new();
    public ObservableCollection<StickerItem> Stickers { get; } = new();
    public ObservableCollection<FileTransferItem> Transfers { get; } = new();
    public ObservableCollection<RoomFileItem> RoomFiles { get; } = new();

    public ChatViewModel(IChatService chatService, MessageCache messageCache, IStickerService stickerService)
    {
        _chatService = chatService;
        _messageCache = messageCache;
        _stickerService = stickerService;
        foreach (var sticker in _stickerService.GetBuiltInStickers())
        {
            Stickers.Add(sticker);
        }

        _chatService.MessageReceived += OnMessageReceived;
        _chatService.PrivateMessageReceived += OnPrivateMessageReceived;
        _chatService.UsersUpdated += OnUsersUpdated;
        _chatService.UserTyping += OnUserTyping;
        _chatService.ConnectionLost += OnConnectionLost;
        _chatService.ConnectionRestored += OnConnectionRestored;
        _chatService.FileTransferUpdated += OnFileTransferUpdated;
        _chatService.RoomFilesUpdated += OnRoomFilesUpdated;

        IsConnected = _chatService.IsConnected;
        if (IsConnected)
        {
            _hasConnectedOnce = true;
            var dispatcher = Application.Current?.Dispatcher;
            if (dispatcher == null)
            {
                _ = InitializeAfterConnectAsync();
            }
            else
            {
                dispatcher.BeginInvoke(() => _ = InitializeAfterConnectAsync());
            }
        }

        InitializeRooms();
        SelectedRoom = Rooms.FirstOrDefault(r => r.Id.Equals(CurrentRoomId, StringComparison.OrdinalIgnoreCase)) ?? Rooms.FirstOrDefault();
        ShowRoomFiles(CurrentRoomId);

        WeakReferenceMessenger.Default.Register(this);

        _typingClearTimer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(3)
        };
        _typingClearTimer.Tick += (s, e) =>
        {
            _typingUsers.Clear();
            UpdateTypingStatus();
            _typingClearTimer.Stop();
        };

        _ = LoadRoomHistoryAsync(CurrentRoomId);
    }

    private async Task LoadRoomHistoryAsync(string roomId)
    {
        try
        {
            var normalizedRoomId = NormalizeRoomId(roomId);
            var history = await _messageCache.GetRoomMessagesAsync(normalizedRoomId);

            var dispatcher = Application.Current?.Dispatcher;
            if (dispatcher == null) return;

            await dispatcher.InvokeAsync(() =>
            {
                var historyMessages = new List<ChatMessage>();

                foreach (var msg in history)
                {
                    DateTime time;
                    if (!DateTime.TryParse(msg.Timestamp, out time)) time = DateTime.Now;

                    historyMessages.Add(CreateMessage(msg.Username, time.ToString("HH:mm"), msg.Content, msg.MessageId));
                }

                MergeRoomHistory(normalizedRoomId, historyMessages);

                if (string.Equals(CurrentRoomId, normalizedRoomId, StringComparison.OrdinalIgnoreCase))
                {
                    ShowRoomMessages(normalizedRoomId);
                }
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CHAT] History load error: {ex.Message}");
        }
    }

    private void InitializeRooms()
    {
        Rooms.Clear();
        _roomLookup.Clear();

        var roomList = new[]
        {
            new RoomItem("General", "# general"),
            new RoomItem("Gaming", "# gaming"),
            new RoomItem("Study", "# study")
        };

        foreach (var room in roomList)
        {
            Rooms.Add(room);
            _roomLookup[room.Id] = room;
            _roomMessages.TryAdd(room.Id, new List<ChatMessage>());
            _roomMessageIds.TryAdd(room.Id, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        }
    }

    private async Task JoinAllRoomsAsync()
    {
        if (Rooms.Count == 0 || _hasJoinedRooms || !_chatService.IsConnected) return;

        foreach (var room in Rooms)
        {
            await _chatService.JoinRoomAsync(room.Id, setActive: false);
        }

        await _chatService.JoinRoomAsync(CurrentRoomId);
        _hasJoinedRooms = true;
    }

    private async Task RejoinRoomsAsync()
    {
        if (Rooms.Count == 0) return;

        await JoinAllRoomsAsync();
        await LoadRoomHistoryAsync(CurrentRoomId);
    }

    partial void OnSelectedRoomChanged(RoomItem? value)
    {
        if (value == null) return;
        _ = SwitchRoomAsync(value.Id);
    }

    private async Task SwitchRoomAsync(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (string.Equals(CurrentRoomId, normalizedRoomId, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        CurrentRoomId = normalizedRoomId;
        _typingUsers.Clear();
        UpdateTypingStatus();
        ShowRoomMessages(normalizedRoomId);
        ShowRoomFiles(normalizedRoomId);

        if (_roomLookup.TryGetValue(normalizedRoomId, out var room))
        {
            room.UnreadCount = 0;
        }

        if (_chatService.IsConnected)
        {
            await _chatService.JoinRoomAsync(normalizedRoomId);
        }

        await LoadRoomHistoryAsync(normalizedRoomId);
    }

    private void ShowRoomFiles(string roomId)
    {
        RoomFiles.Clear();
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_roomFiles.TryGetValue(normalizedRoomId, out var files)) return;

        foreach (var file in files)
        {
            RoomFiles.Add(file);
        }
    }

    private ChatMessage CreateMessage(string sender, string time, string content, string messageId)
    {
        bool isOwn = string.Equals(sender, _chatService.CurrentUsername, StringComparison.Ordinal);
        var sticker = _stickerService.TryGetStickerFromMessage(content);

        if (sticker != null)
        {
            return new ChatMessage(
                sender,
                content,
                time,
                isOwn,
                messageId,
                GetAvatarColor(sender),
                IsSticker: true,
                StickerId: sticker.Id,
                StickerGlyph: sticker.Glyph,
                StickerName: sticker.DisplayName,
                StickerAccentColor: sticker.AccentColor);
        }

        return new ChatMessage(sender, content, time, isOwn, messageId, GetAvatarColor(sender));
    }

    private List<ChatMessage> GetRoomMessages(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_roomMessages.TryGetValue(normalizedRoomId, out var messages))
        {
            messages = new List<ChatMessage>();
            _roomMessages[normalizedRoomId] = messages;
        }

        return messages;
    }

    private HashSet<string> GetRoomMessageIds(string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        if (!_roomMessageIds.TryGetValue(normalizedRoomId, out var ids))
        {
            ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _roomMessageIds[normalizedRoomId] = ids;
        }

        return ids;
    }

    private bool TryAddMessageToRoom(string roomId, ChatMessage message)
    {
        var messages = GetRoomMessages(roomId);
        var ids = GetRoomMessageIds(roomId);
        return TryAddMessage(messages, ids, message);
    }

    private static bool TryAddMessage(List<ChatMessage> messages, HashSet<string> ids, ChatMessage message)
    {
        if (!string.IsNullOrWhiteSpace(message.MessageId) && !ids.Add(message.MessageId))
        {
            return false;
        }

        messages.Add(message);
        return true;
    }

    private void MergeRoomHistory(string roomId, IReadOnlyList<ChatMessage> historyMessages)
    {
        var messages = GetRoomMessages(roomId);
        var existingMessages = messages.ToList();
        var ids = GetRoomMessageIds(roomId);

        messages.Clear();
        ids.Clear();

        foreach (var message in historyMessages)
        {
            TryAddMessage(messages, ids, message);
        }

        foreach (var message in existingMessages)
        {
            TryAddMessage(messages, ids, message);
        }
    }

    private void ShowRoomMessages(string roomId)
    {
        Messages.Clear();

        foreach (var message in GetRoomMessages(roomId))
        {
            Messages.Add(message);
        }
    }

    private static string NormalizeRoomId(string? roomId)
    {
        return string.IsNullOrWhiteSpace(roomId) ? "General" : roomId.Trim();
    }

    public void Receive(ConnectionSuccessMessage message)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null)
        {
            IsConnected = true;
            _ = InitializeAfterConnectAsync();
            return;
        }

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = true;
            _hasConnectedOnce = true;
            _ = InitializeAfterConnectAsync();
        });
    }

    private async Task InitializeAfterConnectAsync()
    {
        await JoinAllRoomsAsync();
        await LoadRoomHistoryAsync(CurrentRoomId);
    }

    private void OnConnectionLost()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = false;
            Users.Clear();
            _knownUsers.Clear();
            OnlineHeader = "OFFLINE";
            _typingUsers.Clear();
            UpdateTypingStatus();
            _hasJoinedRooms = false;
            AddToast("Disconnected", "Connection lost. Reconnecting...", "system", "status");
        });
    }

    private void OnConnectionRestored()
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        var isReconnect = _hasConnectedOnce;
        _hasConnectedOnce = true;

        dispatcher.BeginInvoke(() =>
        {
            IsConnected = true;
            if (isReconnect)
            {
                AddToast("Reconnected", "Connection restored.", "system", "status");
            }
            else
            {
                AddToast("Connected", "Connection established.", "system", "status");
            }
        });

        if (isReconnect)
        {
            _ = RejoinRoomsAsync();
        }
    }

    private void OnRoomFilesUpdated(string roomId, IReadOnlyList<RoomFileDescriptor> files)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        var mapped = files
            .Select(file => new RoomFileItem
            {
                FileId = file.FileId,
                RoomId = normalizedRoomId,
                FileName = file.FileName,
                FileSize = file.FileSize,
                UploadedBy = file.UploadedBy,
                UploadedAt = file.UploadedAt
            })
            .ToList();

        _roomFiles[normalizedRoomId] = mapped;

        if (!string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase)) return;

        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            RoomFiles.Clear();
            foreach (var item in mapped)
            {
                RoomFiles.Add(item);
            }
        });
    }

    private void OnFileTransferUpdated(FileTransferUpdate update)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            var existing = Transfers.FirstOrDefault(t => t.TransferKey == update.TransferKey);
            if (existing == null)
            {
                existing = new FileTransferItem
                {
                    TransferKey = update.TransferKey,
                    TransferId = update.TransferId,
                    RoomId = update.RoomId,
                    Peer = update.Peer,
                    FileName = update.FileName,
                    TotalBytes = update.TotalBytes,
                    TransferredBytes = update.TransferredBytes,
                    Direction = update.Direction,
                    Status = update.Status
                };
                Transfers.Insert(0, existing);
            }

            existing.TransferredBytes = update.TransferredBytes;
            existing.TotalBytes = update.TotalBytes;
            existing.Status = update.Status;
            existing.StatusText = BuildTransferStatusText(update);
            existing.CanCancel = update.Status == FileTransferStatus.InProgress || update.Status == FileTransferStatus.Pending;

            if (update.Direction == FileTransferDirection.Outgoing && update.Status == FileTransferStatus.Completed)
            {
                Transfers.Remove(existing);
            }
        });
    }

    private static string BuildTransferStatusText(FileTransferUpdate update)
    {
        return update.Status switch
        {
            FileTransferStatus.Pending => update.Direction == FileTransferDirection.Outgoing
                ? "Waiting for receiver"
                : "Waiting",
            FileTransferStatus.InProgress => update.Direction == FileTransferDirection.Outgoing
                ? $"Sending to {update.Peer}"
                : $"Receiving from {update.Peer}",
            FileTransferStatus.Completed => "Done",
            FileTransferStatus.Canceled => "Canceled",
            FileTransferStatus.Failed => "Failed",
            _ => ""
        };
    }

    partial void OnMessageTextChanged(string value)
    {
        if (string.IsNullOrEmpty(value)) return;

        if ((DateTime.Now - _lastTypingSent).TotalSeconds > 2)
        {
            _lastTypingSent = DateTime.Now;
            _ = _chatService.SendTypingAsync(true);
        }
    }

    private void OnUserTyping(string username, bool isTyping)
    {
        if (username == _chatService.CurrentUsername) return;

        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            if (isTyping)
            {
                _typingUsers.Add(username);
                _typingClearTimer.Stop();
                _typingClearTimer.Start();
            }
            else
            {
                _typingUsers.Remove(username);
            }

            RefreshKnownUser(username);
            UpdateTypingStatus();
        });
    }

    private void UpdateTypingStatus()
    {
        IsSomeoneTyping = _typingUsers.Count > 0;
        if (!IsSomeoneTyping)
        {
            TypingStatus = "";
            return;
        }

        if (_typingUsers.Count == 1)
        {
            TypingStatus = $"{_typingUsers.First()} is typing...";
        }
        else if (_typingUsers.Count == 2)
        {
            TypingStatus = $"{string.Join(" and ", _typingUsers)} are typing...";
        }
        else
        {
            TypingStatus = "Several people are typing...";
        }
    }

    private void OnMessageReceived(string sender, string time, string content, string messageId, string roomId)
    {
        var normalizedRoomId = NormalizeRoomId(roomId);
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            bool isOwn = sender == _chatService.CurrentUsername;
            Console.WriteLine($"[CHAT] Message from {sender} (room: {normalizedRoomId}, isOwn: {isOwn}): {content}");
            var message = CreateMessage(sender, time, content, messageId);
            var wasAdded = TryAddMessageToRoom(normalizedRoomId, message);

            if (!wasAdded)
            {
                return;
            }

            if (string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase))
            {
                Messages.Add(message);

                // If a message is received from someone typing, remove them from typing list
                if (_typingUsers.Remove(sender))
                {
                    RefreshKnownUser(sender);
                    UpdateTypingStatus();
                }
            }
            else if (_roomLookup.TryGetValue(normalizedRoomId, out var room))
            {
                room.UnreadCount++;
            }

            if (!isOwn && IsMention(content))
            {
                var roomLabel = $"#{normalizedRoomId.ToLowerInvariant()}";
                var preview = string.Equals(normalizedRoomId, CurrentRoomId, StringComparison.OrdinalIgnoreCase)
                    ? $"{sender}: {content}"
                    : $"{roomLabel} - {sender}: {content}";

                AddToast("Mention", preview, sender, "mention");
            }
        });
    }

    private void OnPrivateMessageReceived(string sender, string time, string content, string messageId)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            AddToast("Direct message", $"{sender}: {content}", sender, "dm");
        });
    }

    private void OnUsersUpdated(string[] usernames)
    {
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher == null) return;

        dispatcher.BeginInvoke(() =>
        {
            Console.WriteLine($"[CHAT] Users updated: {string.Join(", ", usernames)}");
            Users.Clear();
            _knownUsers.Clear();
            foreach (var username in usernames)
            {
                var user = CreateUser(username);
                _knownUsers[username] = user;
                Users.Add(user);
            }
            OnlineHeader = $"ONLINE - {Users.Count}";

            // Remove users who left from typing list
            var currentUsers = new HashSet<string>(usernames);
            if (_typingUsers.RemoveWhere(u => !currentUsers.Contains(u)) > 0)
            {
                UpdateTypingStatus();
            }
        });
    }

    [RelayCommand]
    private async Task SendMessage()
    {
        if (string.IsNullOrWhiteSpace(MessageText)) return;

        var message = MessageText;

        try
        {
            var messageId = await _chatService.SendMessageAsync(message);
            if (messageId == null)
            {
                OnMessageReceived("SYSTEM", DateTime.Now.ToString("HH:mm"), "Message was not sent because the connection is not active.", Guid.NewGuid().ToString("N"), CurrentRoomId);
                return;
            }

            OnMessageReceived(_chatService.CurrentUsername ?? "You", DateTime.Now.ToString("HH:mm"), message, messageId, CurrentRoomId);
            MessageText = "";
            IsStickerPickerOpen = false;
            _lastTypingSent = DateTime.MinValue; // Reset so next type sends immediately
        }
        catch (System.Exception)
        {
            // Error handling
        }
    }

    [RelayCommand]
    private async Task DownloadRoomFile(RoomFileItem? item)
    {
        if (item == null) return;
        await _chatService.DownloadRoomFileAsync(item.RoomId, item.FileId, item.FileName);
    }

    [RelayCommand]
    private async Task SendFile()
    {
        if (!_chatService.IsConnected) return;

        var dialog = new OpenFileDialog
        {
            Title = "Select a file to send",
            CheckFileExists = true
        };

        if (dialog.ShowDialog() != true) return;

        var transferId = await _chatService.SendFileAsync(dialog.FileName, CurrentRoomId);
        if (transferId == null)
        {
            OnMessageReceived("SYSTEM", DateTime.Now.ToString("HH:mm"), "File was not sent because the connection is not active.", Guid.NewGuid().ToString("N"), CurrentRoomId);
        }
    }

    [RelayCommand]
    private async Task CancelFileTransfer(string? transferKey)
    {
        if (string.IsNullOrWhiteSpace(transferKey)) return;
        await _chatService.CancelFileTransferAsync(transferKey);
    }

    [RelayCommand]
    private void ToggleStickerPicker()
    {
        IsStickerPickerOpen = !IsStickerPickerOpen;
    }

    [RelayCommand]
    private void InsertQuickEmoji()
    {
        MessageText = string.IsNullOrWhiteSpace(MessageText)
            ? "\uD83D\uDE0A"
            : MessageText + " \uD83D\uDE0A";
    }

    [RelayCommand]
    private async Task SendSticker(string? stickerId)
    {
        if (string.IsNullOrWhiteSpace(stickerId) || !_chatService.IsConnected)
        {
            return;
        }

        try
        {
            var content = _stickerService.CreateStickerMessage(stickerId);
            var messageId = await _chatService.SendMessageAsync(content);
            if (messageId == null)
            {
                OnMessageReceived(
                    "SYSTEM",
                    DateTime.Now.ToString("HH:mm"),
                    "Sticker was not sent because the connection is not active.",
                    Guid.NewGuid().ToString("N"),
                    CurrentRoomId);
                return;
            }

            OnMessageReceived(
                _chatService.CurrentUsername ?? "You",
                DateTime.Now.ToString("HH:mm"),
                content,
                messageId,
                CurrentRoomId);
            IsStickerPickerOpen = false;
        }
        catch (ArgumentException)
        {
            OnMessageReceived(
                "SYSTEM",
                DateTime.Now.ToString("HH:mm"),
                "Unknown sticker.",
                Guid.NewGuid().ToString("N"),
                CurrentRoomId);
        }
    }

    [RelayCommand]
    private void ShowProfile(string? username)
    {
        if (string.IsNullOrWhiteSpace(username)) return;

        var profileName = username == "You" ? _chatService.CurrentUsername ?? username : username;
        var isTyping = _typingUsers.Contains(profileName);
        SelectedProfile = new UserProfileCard(
            profileName,
            GetAvatarColor(profileName),
            isTyping ? "Typing" : "Online",
            isTyping);
        IsProfileCardOpen = true;
    }

    [RelayCommand]
    private void CloseProfile()
    {
        IsProfileCardOpen = false;
        SelectedProfile = null;
    }

    [RelayCommand]
    private void DismissToast(string? toastId)
    {
        if (string.IsNullOrWhiteSpace(toastId)) return;

        var toast = Toasts.FirstOrDefault(t => t.Id == toastId);
        if (toast != null)
        {
            Toasts.Remove(toast);
        }
    }

    private void AddToast(string title, string message, string source, string kind)
    {
        var toast = new ToastNotification(
            Guid.NewGuid().ToString("N"),
            title,
            message,
            source,
            kind,
            DateTime.Now.ToString("HH:mm"));

        Toasts.Insert(0, toast);
        while (Toasts.Count > 4)
        {
            Toasts.RemoveAt(Toasts.Count - 1);
        }

        _ = DismissToastLaterAsync(toast.Id);
    }

    private async Task DismissToastLaterAsync(string toastId)
    {
        await Task.Delay(TimeSpan.FromSeconds(5));
        var dispatcher = Application.Current?.Dispatcher;
        if (dispatcher != null && !dispatcher.HasShutdownStarted)
        {
            await dispatcher.InvokeAsync(() => DismissToast(toastId));
        }
    }

    private bool IsMention(string content)
    {
        var username = _chatService.CurrentUsername;
        return !string.IsNullOrWhiteSpace(username) &&
            content.Contains("@" + username, StringComparison.OrdinalIgnoreCase);
    }

    private User CreateUser(string username)
    {
        var isTyping = _typingUsers.Contains(username);
        return new User(username, GetAvatarColor(username), isTyping ? "Typing" : "Online", isTyping);
    }

    private void RefreshKnownUser(string username)
    {
        if (!_knownUsers.ContainsKey(username)) return;

        var updatedUser = CreateUser(username);
        _knownUsers[username] = updatedUser;

        var index = Users.ToList().FindIndex(u => string.Equals(u.Name, username, StringComparison.OrdinalIgnoreCase));
        if (index >= 0)
        {
            Users[index] = updatedUser;
        }
    }

    private static string GetAvatarColor(string username)
    {
        var colors = new[]
        {
            "#5865F2",
            "#43B581",
            "#F04747",
            "#FAA61A",
            "#00A8CC",
            "#9B59B6",
            "#2ECC71",
            "#E67E22"
        };

        var hash = StringComparer.OrdinalIgnoreCase.GetHashCode(username);
        var index = (int)((uint)hash % colors.Length);
        return colors[index];
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _chatService.MessageReceived -= OnMessageReceived;
            _chatService.PrivateMessageReceived -= OnPrivateMessageReceived;
            _chatService.UsersUpdated -= OnUsersUpdated;
            _chatService.UserTyping -= OnUserTyping;
            _chatService.ConnectionLost -= OnConnectionLost;
            _chatService.ConnectionRestored -= OnConnectionRestored;
            _chatService.FileTransferUpdated -= OnFileTransferUpdated;
            _chatService.RoomFilesUpdated -= OnRoomFilesUpdated;
            WeakReferenceMessenger.Default.Unregister<ConnectionSuccessMessage>(this);
            _typingClearTimer.Stop();
            _disposed = true;
        }
    }
}
