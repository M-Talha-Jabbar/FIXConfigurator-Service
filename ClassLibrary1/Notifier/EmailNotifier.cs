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
        public static Dictionary<string, int> recurringEmailsCount = new Dictionary<string, int>();

        public EmailNotifier(string conId, string status, Sessions sessionInfo) // Email Alert without Timer
        {
            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;
        }
        public EmailNotifier(int interval, string conId, string status, Sessions sessionInfo) // Email Alert with Time defined (can be both Recurring & Non-Recurring)
        {
            timer = new Timer(interval);
            timer.Elapsed += OnEventExecution;

            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;

            timer.AutoReset = (bool)sessionInfo.Recurring; // By default recurring emails are disabled.
            if (timer.AutoReset)
                recurringEmailsCount.Add(conId, 0);

            timer.Start();
        }

        private void OnEventExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            SendEmail();

            if (!(bool)sessionInfo.Recurring)
                emailTimer.Remove(conId);
            else
                recurringEmailsCount[conId]++;
        }

        public EmailNotifier SendEmail()
        {
            emailHandler.SendEmail(conId, status, sessionInfo);
            return this;
        }

        public Timer getTimerInstance()
        {
            return timer;
        }
    }
}
