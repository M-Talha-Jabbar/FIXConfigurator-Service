using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXSession
    {
        public string ConnectionID { get; set; }

        public string Status { get; set; } = "disconnected";

        public string SenderCompID { get; set; }

        public string TargetCompID { get; set; }

        public string IPAddress { get; set; }

        public string BackUpIPAddress { get; set; }

        public ulong Port { get; set; }

        public ulong BackUpPort { get; set; }

        public bool Validate { get; set; }

        public bool HandleResend { get; set; }

        public ulong HeartBeatInterval { get; set; }

        public ulong MaxLatency { get; set; }

        public bool ResetConnection { get; set; }

        public bool EnableConnection { get; set; }

        public string FIXVersion { get; set; }

        public string InternalFIXVersion { get; set; }

        public string Mode { get; set; }

        public bool DBEnabled { get; set; }

        public bool LatencyEnabled { get; set; }

        public bool AutoConnect { get; set; }

        public bool AutoReconnect { get; set; }

        public ulong ReconnectDelay { get; set; }

        public ulong ConnectRetry { get; set; }

        public string LogonRawData { get; set; }

        public bool MilliSecondTime { get; set; }

        public bool? QEnabled { get; set; }

        public DateTime SessionStart { get; set; }

        public DateTime SessionEnd { get; set; }

        public string TaskReset { get; set; }

        public ulong InSeqNum { get; set; }

        public ulong OutSeqNum { get; set; }

        public DateTime LastUpdated { get; set; }

        public List<FIXMessage> FixMessages { get; set; } = new List<FIXMessage>();

        public static implicit operator FIXSession(proto.Config config)
        {
            return new FIXSession
            {
                ConnectionID = config.ConnectionID,

                Status = config.Status.ToString(),

                SenderCompID = config.SenderCompID,

                TargetCompID = config.TargetCompID,

                IPAddress = config.IPAddress,

                BackUpIPAddress = config.BackUpIPAddress,

                Port = config.Port,

                BackUpPort = config.BackUpPort,

                Validate = config.Validate,

                HandleResend = config.HandleResend,

                HeartBeatInterval = config.HeartBeatInterval,

                MaxLatency = config.MaxLatency,

                ResetConnection = config.ResetConnection,

                EnableConnection = config.EnableConnection,

                FIXVersion = config.FIXVersion,

                InternalFIXVersion = config.InternalFIXVersion,

                Mode = config.Mode,

                DBEnabled = config.DBEnabled,

                LatencyEnabled = config.LatencyEnabled,

                AutoConnect = config.AutoConnect,

                AutoReconnect = config.AutoReconnect,

                ReconnectDelay = config.ReconnectDelay,

                ConnectRetry = config.ConnectRetry,

                LogonRawData = config.LogonRawData,

                MilliSecondTime = config.MilliSecondTime,

                QEnabled = config.QEnabled,

                SessionStart = new DateTime((long)config.SessionStart),

                SessionEnd = new DateTime((long)config.SessionEnd),

                TaskReset = config.TaskReset,

                InSeqNum = config.InSeqNum,

                OutSeqNum = config.OutSeqNum,

                LastUpdated = new DateTime((long)config.LastUpdated)

            };
        }

        public static implicit operator proto.Config(FIXSession session)
        {
            return new proto.Config
            {
                ConnectionID = session.ConnectionID,

                Status = session.Status.Equals("CONNECTED", StringComparison.OrdinalIgnoreCase) ? proto.MessageStatus.CONNECTED : session.Status.Equals("DISCONNECTED", StringComparison.OrdinalIgnoreCase) ? proto.MessageStatus.DISCONNECTED : session.Status.Equals("UNAVAILABLE", StringComparison.OrdinalIgnoreCase) ? proto.MessageStatus.UNAVAILABLE : proto.MessageStatus.Default,

                SenderCompID = session.SenderCompID,

                TargetCompID = session.TargetCompID,

                IPAddress = session.IPAddress,

                BackUpIPAddress = session.BackUpIPAddress,

                Port = session.Port,

                BackUpPort = session.BackUpPort,

                Validate = session.Validate,

                HandleResend = session.HandleResend,

                HeartBeatInterval = session.HeartBeatInterval,

                MaxLatency = session.MaxLatency,

                ResetConnection = session.ResetConnection,

                EnableConnection = session.EnableConnection,

                FIXVersion = session.FIXVersion == null ? "" : session.FIXVersion,

                InternalFIXVersion = session.InternalFIXVersion == null ? "" : session.InternalFIXVersion,

                Mode = session.Mode == null ? "" : session.Mode,

                DBEnabled = session.DBEnabled,

                LatencyEnabled = session.LatencyEnabled,

                AutoConnect = session.AutoConnect,

                AutoReconnect = session.AutoReconnect,

                ReconnectDelay = session.ReconnectDelay,

                ConnectRetry = session.ConnectRetry,

                LogonRawData = session.LogonRawData == null ? "" : session.LogonRawData,

                MilliSecondTime = session.MilliSecondTime,

                QEnabled = session.QEnabled.HasValue ? session.QEnabled.Value : false,

                SessionStart = (ulong)session.SessionStart.Ticks,

                SessionEnd = (ulong)session.SessionEnd.Ticks,

                TaskReset = session.TaskReset == null ? "" : session.TaskReset,

                InSeqNum = session.InSeqNum,

                OutSeqNum = session.OutSeqNum,

                LastUpdated = (ulong)session.LastUpdated.Ticks
            };
        }

    }
}
