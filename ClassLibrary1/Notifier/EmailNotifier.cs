using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using GEmail;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Timers;

namespace FIXMonitorBusinessLogicLayer.Notifier
{
    class EmailNotifier
    {
        private Timer timer;
        private static EmailHandler emailHandler = new EmailHandler();
        private string conId;
        private string status;
        private FixSessions sessionInfo;
        private FIXSession FIXSession;
        private FIXEngine fixEngine;
        private FixEnginesKeyedCollection FIXEngines;
        private FixTagValues fixTagValues; 

        public static Dictionary<string, Timer> emailTimer = new Dictionary<string, Timer>();
        public static Dictionary<string, int> recurringEmailsCount = new Dictionary<string, int>();
        public static List<FixTagValues> fixTagValueConfigurations = new List<FixTagValues>();

        private readonly string DefaultCommaSeperatedToEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedToEmails"].ToString();
        private readonly string DefaultCommaSeperatedCCEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedCCEmails"].ToString();
        private readonly string Environment = System.Configuration.ConfigurationManager.AppSettings["Environment"].ToString();

        public EmailNotifier(string conId, string status, FixSessions sessionInfo) // Email Alert without Timer
        {
            this.conId = conId;
            this.status = status;
            this.sessionInfo = sessionInfo;
        }

        public EmailNotifier(int interval, string conId, string status, FixSessions sessionInfo) // Email Alert with Time defined (can be both Recurring & Non-Recurring)
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

        public EmailNotifier(int interval, FIXSession fixSession, FixSessions sessionInfo) // Scheduled Check for FixSession Status
        {
            timer = new Timer(interval);
            timer.Elapsed += OnFixSessionScheduledCheckExecution;
            timer.AutoReset = false;

            this.conId = fixSession.ConnectionID;
            this.sessionInfo = sessionInfo;
            this.FIXSession = fixSession;

            timer.Start();
        }

        public EmailNotifier(int interval, FixEnginesKeyedCollection FIXEngines) // Scheduled Check for FixEngines Status
        {
            timer = new Timer(interval);
            timer.Elapsed += OnFixEnginesScheduledCheckExecution;
            timer.AutoReset = false;

            this.FIXEngines = FIXEngines;

            timer.Start();
        }

        public EmailNotifier(FIXEngine fixEngine) // Email Alert for Redis Disconnect & then on Reconnect
        {
            this.fixEngine = fixEngine;
        }

        public EmailNotifier(string conId, FixTagValues fixTagValues) // Email Alert for Fix Message Having Any Configured Tag/Value Pair
        {
            this.conId = conId;
            this.fixTagValues = fixTagValues;
        }

        private void OnEventExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            SendEmail(Email.FixSession);

            if (!(bool)sessionInfo.Recurring)
                emailTimer.Remove(conId);
            else
                recurringEmailsCount[conId]++;
        }

        private void OnFixSessionScheduledCheckExecution(Object sender, ElapsedEventArgs eventArgs)
        {
            if(FIXSession.Status.Equals("Disconnected", StringComparison.OrdinalIgnoreCase))
            {
                this.status = FIXSession.Status;
                SendEmail(Email.FixSession);
            }
        }

        private void OnFixEnginesScheduledCheckExecution(Object sender, ElapsedEventArgs eventArgs) => SendEmail(Email.FixEngines);

        public EmailNotifier SendEmail(Email emailEnum)
        {
            EmailData emailData = new EmailData();

            switch (emailEnum)
            {
                case Email.FixEngines:

                    emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                    emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                    emailData.Subject = "FixEngines Status Alert";
                    emailData.Body = FIXEngines.Count > 0 ? createTemplateForFixEnginesStatusAlert() : "No Engines are there in FIXConfigurator";

                    break;

                case Email.FixSession:

                    if (sessionInfo == null || string.IsNullOrEmpty(sessionInfo.ToEmails))
                    {
                        emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                        emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                        emailData.Subject = $"Session {conId} status changed";
                        emailData.Body = $"Session {conId} status changed to {status} -> {Environment} Environment";
                    }
                    else
                    {
                        emailData.CommaSeperatedToEmails = sessionInfo.ToEmails;
                        emailData.CommaSeperatedCCEmails = sessionInfo.CcEmails;
                        emailData.Subject = string.IsNullOrEmpty(sessionInfo.Subject) ? $"Session {conId} status changed" : Regex.Replace(Regex.Replace(Regex.Replace(sessionInfo.Subject, "{sessionId}", conId, RegexOptions.IgnoreCase), "{status}", status, RegexOptions.IgnoreCase), "{environment}", Environment, RegexOptions.IgnoreCase);
                        emailData.Body = string.IsNullOrEmpty(sessionInfo.Body) ? $"Session {conId} status changed to {status} -> {Environment} Environment" : Regex.Replace(Regex.Replace(Regex.Replace(sessionInfo.Body, "{sessionId}", conId, RegexOptions.IgnoreCase), "{status}", status, RegexOptions.IgnoreCase), "{environment}", Environment, RegexOptions.IgnoreCase);
                    }

                    break;

                case Email.FixMessageReject:

                    if (string.IsNullOrEmpty(fixTagValues.ToEmails))
                    {
                        emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                        emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                        emailData.Subject = $"Session {conId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue})";
                        emailData.Body = $"Session {conId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue}) -> {Environment} Environment";
                    }
                    else
                    {
                        emailData.CommaSeperatedToEmails = fixTagValues.ToEmails;
                        emailData.CommaSeperatedCCEmails = fixTagValues.CcEmails;
                        emailData.Subject = string.IsNullOrEmpty(fixTagValues.Subject) ? $"Session {conId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue})" : Regex.Replace(Regex.Replace(Regex.Replace(fixTagValues.Subject, "{sessionId}", conId, RegexOptions.IgnoreCase), "{FixTag}", fixTagValues.FixTag, RegexOptions.IgnoreCase), "{FixValue}", fixTagValues.FixValue, RegexOptions.IgnoreCase);
                        emailData.Body = string.IsNullOrEmpty(fixTagValues.Body) ? $"Session {conId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue}) -> {Environment} Environment" : Regex.Replace(Regex.Replace(Regex.Replace(fixTagValues.Body, "{sessionId}", conId, RegexOptions.IgnoreCase), "{FixTag}", fixTagValues.FixTag, RegexOptions.IgnoreCase), "{FixValue}", fixTagValues.FixValue, RegexOptions.IgnoreCase);
                    }

                    break;

                case Email.RedisDisconnect:

                    emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                    emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                    emailData.Subject = $"{fixEngine.engineName} Redis Disconnected";
                    emailData.Body = $"{fixEngine.engineName} has lost subscription to Redis.\nRedis IP: {fixEngine.redisIpAddress}\nRedis Port: {fixEngine.redisIpPort}\nRedis DB: {fixEngine.redisDB}";

                    break;

                case Email.RedisReconnect:

                    emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                    emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                    emailData.Subject = $"{fixEngine.engineName} Redis Reconnected";
                    emailData.Body = $"{fixEngine.engineName} has resubscribed to Redis.\nRedis IP: {fixEngine.redisIpAddress}\nRedis Port: {fixEngine.redisIpPort}\nRedis DB: {fixEngine.redisDB}";

                    break;
            }

            emailHandler.DispatchEmail(emailData);

            return this;
        }
        private string createTemplateForFixEnginesStatusAlert()
        {
            string template = "";

            foreach (var engine in FIXEngines)
            {
                bool instance = SocketListener.fixEngineSocketConnections.TryGetValue(engine.engineID, out SocketListener value);
                string status = value.isConnected ? "Running" : "Stopped";

                template += $"Engine Name : {engine.engineName}\nEngine Status : {status}\n\n\n";
            }

            return template;
        }

        public Timer getTimerInstance() => timer;

        public static void DisposeEmailTimer(string sessionId)
        {
            Timer timer;
            emailTimer.TryGetValue(sessionId, out timer);
            timer.Stop();
            timer.Dispose();

            emailTimer.Remove(sessionId);
        }

        public enum Email
        {
            FixEngines,
            FixSession,
            FixMessageReject,
            RedisDisconnect,
            RedisReconnect
        }
    }
}
