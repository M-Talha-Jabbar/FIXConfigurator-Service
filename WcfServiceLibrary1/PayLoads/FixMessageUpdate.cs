using CoreLogging;
using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public class FixMessageUpdate : IUpdate
    {
        public FIXMessage fixMessage { get; set; }
        public string engineID { get; set; }
        public string sessionID { get; set; }

        public FixMessageUpdate(FIXMessage fixMessage, string engineID, string sessionID)
        {
            this.fixMessage = fixMessage;
            this.engineID = engineID;
            this.sessionID = sessionID;
        }

        public void SendUpdateToClient(IFIXMonitorServiceCallback callback)
        {
            callback.SendFixMessagesToClient(fixMessage, engineID, sessionID);
            Logging.LogMessage(LOGTYPE.Info, "Sent FixMessageUpdate in Queue");
        }
    }
}
