using System;
using System.Collections.Generic;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace FIXMonitorBusinessLogicLayer.Data
{
    public partial class Sessions
    {
        public string SessionId { get; set; }
        public string ToEmails { get; set; }
        public string CcEmails { get; set; }
        public string EmailStatus { get; set; }
        public DateTime Timeout { get; set; }
    }
}
