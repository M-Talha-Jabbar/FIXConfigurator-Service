using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.Handler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private FIXSession FIXSession;
        private FixTagValues fixTagValues; 

        public static Dictionary<string, Timer> emailTimer = new Dictionary<string, Timer>();
        public static Dictionary<string, int> recurringEmailsCount = new Dictionary<string, int>();
        public static List<FixTagValues> fixTagValueConfigurations = new List<FixTagValues>();

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

        public EmailNotifier(int interval, FIXSession fixSession, Sessions sessionInfo) // Scheduled Check
        {
            timer = new Timer(interval);
            timer.Elapsed += OnScheduledCheckExecution;
            timer.AutoReset = false;

            this.conId = sessionInfo.SessionId;
            this.sessionInfo = sessionInfo;
            this.FIXSession = fixSession;

            timer.Start();
        }

        public EmailNotifier(string conId, FixTagValues fixTagValues) // Email Alert for Fix Message Having Any Configured Tag/Value Pair
        {
            this.conId = conId;
            this.fixTagValues = fixTagValues;
        }

        private void OnEventExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            SendEmail();

            if (!(bool)sessionInfo.Recurring)
                emailTimer.Remove(conId);
            else
                recurringEmailsCount[conId]++;
        }

        private void OnScheduledCheckExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            if(FIXSession.Status.Equals("Disconnected", StringComparison.OrdinalIgnoreCase))
            {
                this.status = FIXSession.Status;
                SendEmail();
            }
        }

        public EmailNotifier SendEmail()
        {
            emailHandler.SendEmail(conId, status, sessionInfo);
            return this;
        }

        public EmailNotifier SendEmailForFIXMessageReject()
        {
            emailHandler.SendEmail(conId, fixTagValues);
            return this;
        }

        public Timer getTimerInstance()
        {
            return timer;
        }

        public static void DisposeEmailTimer(string sessionId)
        {
            Timer timer;
            emailTimer.TryGetValue(sessionId, out timer);
            timer.Stop();
            timer.Dispose();

            emailTimer.Remove(sessionId);
        }
    }
}
