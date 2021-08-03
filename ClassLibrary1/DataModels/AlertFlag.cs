using System;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public enum AlertPriority
    {
        Low = 0,
        Medium = 1,
        High = 2
    }
    public class AlertFlag
    {
        public string orderId { get; set; }
        public string message { get; set; }
        public AlertPriority alertPriority { get; set; }
        public DateTime dateTime { get; set; } = DateTime.Now;
        public bool allowUI { get; set; } = true;
    }
}
