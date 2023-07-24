using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using GEmail;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    public interface IEmailHandler
    {
        void DispatchEmail(EmailData emailData);
        void SendEmail(string sessionId, string status, FixSessions sessionInfo);
        void SendEmail(string sessionId, FixTagValues fixTagValues);
        void SendEmail(FixEnginesKeyedCollection FIXEngines);
        SessionEmails GetSessionAlertConfiguration(string SessionId);
        bool AddSessionAlertConfiguration(SessionEmails sessionEmails);
        bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails);
        bool DeleteSessionAlertConfiguration(string SessionId);
        List<FixTagValueConfiguration> GetAllFixMessageConfiguration();
        bool AddFixMessageConfiguration(FixTagValueConfiguration fixTagValueConfiguration);
        bool DeleteFixMessageConfiguration(int id);
    }
}
