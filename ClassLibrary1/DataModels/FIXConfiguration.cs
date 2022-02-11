using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXConfiguration
    {
        [Required]
        public string ConnectionID { get; set; }
        
        [Required]
        public string Status { get; set; }
        
        [Required]
        public string SenderID { get; set; }

        [Required]
        public string TargetID { get; set; }

        [Required]
        public string IPAddress { get; set; }

        [Required]
        public string BackUpIPAddress { get; set; }

        [Required]
        public int BackUpPort { get; set; }

        [Required]
        public bool Validate { get; set; }

        [Required]
        public int Port { get; set; }

        [Required]
        public bool HandleResend { get; set; }

        [Required]
        public int HeartBeartInterval { get; set; }

        [Required]
        public int MaxLatency { get; set; }

        [Required]
        public bool ResetConnection { get; set; }

        [Required]
        public bool EnableConnection { get; set; }

        [Required]
        public string FIXVersion { get; set; }

        [Required]
        public string InternalFIXVersion { get; set; }

        [Required]
        public string Mode { get; set; }

        [Required]
        public bool DBEnabled { get; set; }

        [Required]
        public bool LatencyEnabled { get; set; }

        [Required]
        public bool AutoConnect { get; set; }

        [Required]
        public bool AutoReconnect { get; set; }

        [Required]
        public int ReconnectDelay { get; set; }

        public int ConnectRetry { get; set; }

        public string LogonRawData { get; set; }

        public bool MilliSecondTime { get; set; }

        public bool? QEnabled { get; set; }

        public DateTime SessionStart { get; set; }

        public DateTime SessionEnd { get; set; }

        public string TaskReset { get; set; }

        public int InSeqNum { get; set; }
        
        public int OutSeqNum { get; set; }

        public DateTime LastUpdated { get; set; }
    }
}
