using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public class FixSessionStatusUpdate : IUpdate
    {
        public string fixSessionStatusMessage { get; set; }

        public FixSessionStatusUpdate(string fixSessionStatusMessage)
        {
            this.fixSessionStatusMessage = fixSessionStatusMessage;
        }
        public void SendUpdateToClient(IFIXMonitorServiceCallback callback)
        {
            callback.SendFixSessionStatusMessage(fixSessionStatusMessage);
            Console.WriteLine("Sent FixSessionStatusUpdate in Queue");
        }
    }
}
