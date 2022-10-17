using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class SessionEmails
    {
        public string SessionId { get; set; }
        public string ToEmails { get; set; }
        public string CcEmails { get; set; }
        public bool EmailStatus { get; set; }
        public DateTime Timeout { get; set; }
        public bool? Recurring { get; set; }
    }
}
