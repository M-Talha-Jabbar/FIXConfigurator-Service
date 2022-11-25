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
        private FixmessageRejects fixmessageRejects; 

        public static Dictionary<string, Timer> emailTimer = new Dictionary<string, Timer>();
        public static Dictionary<string, int> recurringEmailsCount = new Dictionary<string, int>();
        public static List<FixmessageRejects> FixmsgRejects = new List<FixmessageRejects>();

        public EmailNotifier(string conId, string status, Sessions sessionInfo) 
        {
            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;
        }

        public EmailNotifier(int interval, string conId, string status, Sessions sessionInfo) 
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

        public EmailNotifier(int interval, FIXSession fixSession, Sessions sessionInfo) 
        {
            timer = new Timer(interval);
            timer.Elapsed += OnScheduledCheckExecution;
            timer.AutoReset = false;

            this.conId = sessionInfo.SessionId;
            this.sessionInfo = sessionInfo;
            this.FIXSession = fixSession;

            timer.Start();
        }

        public EmailNotifier(string conId, FixmessageRejects fixmessageRejects)
        {
            this.conId = conId;
            this.fixmessageRejects = fixmessageRejects;
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
            emailHandler.SendEmail(conId, fixmessageRejects);
            return this;
        }

        public Timer getTimerInstance()
        {
            return timer;
        }
    }
}
