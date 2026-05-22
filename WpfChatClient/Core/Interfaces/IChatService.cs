using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using WpfChatClient.Core.Models;

namespace WpfChatClient.Core.Interfaces;

public delegate void MessageReceivedHandler(string sender, string time, string content, string messageId, string roomId);
public delegate void PrivateMessageReceivedHandler(string sender, string time, string content, string messageId);
public delegate void UsersUpdatedHandler(string[] users);
public delegate void UserTypingHandler(string username, bool isTyping);
public delegate void ConnectionStateHandler();
public delegate void FileTransferUpdatedHandler(FileTransferUpdate update);
public delegate void RoomFilesUpdatedHandler(string roomId, IReadOnlyList<RoomFileDescriptor> files);

public interface IChatService
{
    event MessageReceivedHandler MessageReceived; // sender, time, content, messageId, roomId
    event PrivateMessageReceivedHandler PrivateMessageReceived; // sender, time, content, messageId
    event UsersUpdatedHandler UsersUpdated;
    event UserTypingHandler UserTyping;
    event ConnectionStateHandler ConnectionLost;
    event ConnectionStateHandler ConnectionRestored;
    event FileTransferUpdatedHandler FileTransferUpdated;
    event RoomFilesUpdatedHandler RoomFilesUpdated;
    
    string? CurrentUsername { get; }
    bool IsConnected { get; }
    Task ConnectAsync(string ip, int port, string username);
    Task JoinRoomAsync(string roomId, bool setActive = true);
    Task<string?> SendMessageAsync(string message);
    Task<string?> SendFileAsync(string filePath, string roomId);
    Task DownloadRoomFileAsync(string roomId, string fileId, string fileName);
    Task SendTypingAsync(bool isTyping);
    Task CancelFileTransferAsync(string transferKey);
    void Disconnect();
}
