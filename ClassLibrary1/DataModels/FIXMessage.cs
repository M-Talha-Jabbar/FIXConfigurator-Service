using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXMessage
    {
        public string fixMessage { get; set; }
        public string messageType { get; set; }
        public string sendingTime { get; set; }
        public List<Tuple<string, string, string>> keyValuePair { get; set; }
    }
}
