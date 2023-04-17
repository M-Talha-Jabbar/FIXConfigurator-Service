using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FixTagValueConfiguration
    {
        public int Id { get; set; }
        public string FixTag { get; set; }
        public string FixValue { get; set; }
        public string ToEmails { get; set; }
        public string CcEmails { get; set; }
        public bool EmailStatus { get; set; }
        public string Subject { get; set; }
        public string Body { get; set; }
        public string Engine { get; set; }
        public string SessionId { get; set; }
    }
}
