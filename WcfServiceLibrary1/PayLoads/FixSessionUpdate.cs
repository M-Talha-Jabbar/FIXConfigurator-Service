using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public class FixSessionUpdate : IUpdate
    {
        public FIXSession fixSession { get; set; }
        public string engineID { get; set; }
        public string commandType { get; set; }

        public FixSessionUpdate(FIXSession fixSession, string engineID, string commandType)
        {
            this.fixSession = fixSession;
            this.engineID = engineID;
            this.commandType = commandType;
        }

        public void SendUpdateToClient(IFIXMonitorServiceCallback callback)
        {
            callback.SendFixSessionToClient(fixSession.GetClone(), engineID, commandType);
            Console.WriteLine("Sent FixSessionUpdate in Queue");
        }
    }
}
