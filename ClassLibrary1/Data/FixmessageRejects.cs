using System;
using System.Collections.Generic;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace FIXMonitorBusinessLogicLayer.Data
{
    public partial class FixmessageRejects
    {
        public int Id { get; set; }
        public string FixTag { get; set; }
        public string FixValue { get; set; }
        public string ToEmails { get; set; }
        public string CcEmails { get; set; }
        public bool EmailStatus { get; set; }
        public string Subject { get; set; }
        public string Body { get; set; }
    }
}
