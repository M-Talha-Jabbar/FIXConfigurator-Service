using System;
using System.Collections.Generic;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace FIXMonitorBusinessLogicLayer.Data
{
    public partial class FixSessions
    {
        public string SessionId { get; set; }
        public string ToEmails { get; set; }
        public string CcEmails { get; set; }
        public bool EmailStatus { get; set; }
        public DateTime Timeout { get; set; }
        public bool? Recurring { get; set; }
        public string Body { get; set; }
        public string Subject { get; set; }
    }
}
