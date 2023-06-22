using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXSessionsConnectivityStatus
    {
        public string engineID { get; set; }
        public string engineName { get; set; }
        public string ConnectionID { get; set; }
        public string Status { get; set; }
        public string Mode { get; set; }
    }
}
