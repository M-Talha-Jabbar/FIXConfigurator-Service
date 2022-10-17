using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    public interface IEmailHandler
    {
        void SendEmail(string sessionId, string status, Sessions sessionInfo);
        SessionEmails GetSessionAlertConfiguration(string SessionId);
        bool AddSessionAlertConfiguration(SessionEmails sessionEmails);
        bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails);
        bool DeleteSessionAlertConfiguration(string SessionId);
    }
}
