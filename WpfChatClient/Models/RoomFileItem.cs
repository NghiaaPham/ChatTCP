using System;
using CommunityToolkit.Mvvm.ComponentModel;

namespace WpfChatClient.Models;

public partial class RoomFileItem : ObservableObject
{
    [ObservableProperty]
    private string _fileId = string.Empty;

    [ObservableProperty]
    private string _roomId = string.Empty;

    [ObservableProperty]
    private string _fileName = string.Empty;

    [ObservableProperty]
    private long _fileSize;

    [ObservableProperty]
    private string _uploadedBy = string.Empty;

    [ObservableProperty]
    private string _uploadedAt = string.Empty;

    public string FileSizeLabel
    {
        get
        {
            if (FileSize < 1024) return $"{FileSize} B";
            if (FileSize < 1024 * 1024) return $"{FileSize / 1024.0:F1} KB";
            if (FileSize < 1024 * 1024 * 1024) return $"{FileSize / (1024.0 * 1024.0):F1} MB";
            return $"{FileSize / (1024.0 * 1024.0 * 1024.0):F2} GB";
        }
    }

    partial void OnFileSizeChanged(long value)
    {
        OnPropertyChanged(nameof(FileSizeLabel));
    }
}
