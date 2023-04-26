using CoreLogging;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.PayLoads
{
    public class JenkinsJobUpdate : IUpdate
    {

        JenkinsJobStatus _jenkinsJobStatus;

        public JenkinsJobUpdate(JenkinsJobStatus jenkinsJobStatus)
        {
            _jenkinsJobStatus = jenkinsJobStatus;
        }

        public void SendUpdateToClient(IFIXMonitorServiceCallback callback)
        {
            callback.SendJenkinsJobUpdate(_jenkinsJobStatus);
            Logging.LogMessage(LOGTYPE.Info, "Sent Jenkins Job Status Update from Queue");
        }
    }
}
