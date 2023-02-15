using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public interface IUpdate
    {
        void SendUpdateToClient(IFIXMonitorServiceCallback callback);
    }
}
