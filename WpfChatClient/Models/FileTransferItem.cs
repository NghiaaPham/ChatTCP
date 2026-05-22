using System;
using CommunityToolkit.Mvvm.ComponentModel;
using WpfChatClient.Core.Models;

namespace WpfChatClient.Models;

public partial class FileTransferItem : ObservableObject
{
    [ObservableProperty]
    private string _transferKey = string.Empty;

    [ObservableProperty]
    private string _transferId = string.Empty;

    [ObservableProperty]
    private string _roomId = string.Empty;

    [ObservableProperty]
    private string _peer = string.Empty;

    [ObservableProperty]
    private string _fileName = string.Empty;

    [ObservableProperty]
    private long _totalBytes;

    [ObservableProperty]
    private long _transferredBytes;

    [ObservableProperty]
    private FileTransferDirection _direction;

    [ObservableProperty]
    private FileTransferStatus _status;

    [ObservableProperty]
    private string _statusText = string.Empty;

    [ObservableProperty]
    private bool _canCancel;

    public int ProgressPercent
    {
        get
        {
            if (TotalBytes <= 0) return 0;
            var ratio = (double)TransferredBytes / TotalBytes;
            return Math.Max(0, Math.Min(100, (int)Math.Round(ratio * 100)));
        }
    }

    public string ProgressText
    {
        get
        {
            if (TotalBytes <= 0) return "0%";
            return $"{ProgressPercent}%";
        }
    }

    partial void OnTransferredBytesChanged(long value)
    {
        OnPropertyChanged(nameof(ProgressPercent));
        OnPropertyChanged(nameof(ProgressText));
    }

    partial void OnTotalBytesChanged(long value)
    {
        OnPropertyChanged(nameof(ProgressPercent));
        OnPropertyChanged(nameof(ProgressText));
    }
}
