using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.Handler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace FIXMonitorBusinessLogicLayer.Notifier
{
    class EmailNotifier
    {
        private Timer timer;
        private static EmailHandler emailHandler = new EmailHandler();
        private string conId;
        private string status;
        private Sessions sessionInfo;

        public static Dictionary<string, Timer> emailTimer = new Dictionary<string, Timer>();

        public EmailNotifier(string conId, string status, Sessions sessionInfo) // Email Alert without Timer
        {
            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;
        }
        public EmailNotifier(int interval, string conId, string status, Sessions sessionInfo) // Email Alert with Time defined
        {
            timer = new Timer(interval);
            timer.Elapsed += OnEventExecution;
            timer.AutoReset = false; // Disable recurrent events.
            timer.Start();

            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;
        }

        private void OnEventExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            SendEmail();

            emailTimer.Remove(conId); 
        }

        public void SendEmail()
        {
            emailHandler.SendEmail(conId, status, sessionInfo);
        }

        public Timer getTimerInstance()
        {
            return timer;
        }
    }
}
