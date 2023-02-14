using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public class ConfiguredFixMessage : IUpdate
    {
        public FIXMessage fixMessage { get; set; }
        public string engineID { get; set; }
        public string sessionID { get; set; }

        public ConfiguredFixMessage(FIXMessage fixMessage, string engineID, string sessionID) 
        {
            this.fixMessage = fixMessage;
            this.engineID = engineID;
            this.sessionID = sessionID;
        }

        public void SendUpdateToClient(IFIXMonitorServiceCallback callback)
        {
            callback.SendFixMessageWithConfiguredFixTagValuePairToClient(fixMessage, engineID, sessionID);
            Console.WriteLine("Sent FixMessageUpdate (i.e. for ConfiguredFixTagValuePair) in Queue");
        }
    }
}
