using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Converter;
//using EmailSender;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.Notifier;
using GEmail;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    class EmailHandler : IEmailHandler
    {
        //private EmailService emailService;
        //private readonly string FromEmail = System.Configuration.ConfigurationManager.AppSettings["FromEmail"].ToString();
        //private readonly string EmailApiKey = System.Configuration.ConfigurationManager.AppSettings["EmailApiKey"].ToString();

        private readonly string DefaultCommaSeperatedToEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedToEmails"].ToString();
        private readonly string DefaultCommaSeperatedCCEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedCCEmails"].ToString();
        private readonly string Environment = System.Configuration.ConfigurationManager.AppSettings["Environment"].ToString();

        private EmailNotifier emailNotifier;
        public EmailHandler()
        {
            //emailService = new EmailService(EmailApiKey);
            //GEmailUtil.ConfigureMail();
        }

        public void SendEmail(string sessionId, string status, Sessions sessionInfo)
        {
            EmailData emailData = new EmailData();

            if (string.IsNullOrEmpty(sessionInfo.ToEmails))
            {
                emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                emailData.Subject = $"Session {sessionId} status changed";
                emailData.Body = $"Session {sessionId} status changed to {status} -> {Environment} Environment";
            }
            else
            {
                emailData.CommaSeperatedToEmails = sessionInfo.ToEmails;
                emailData.CommaSeperatedCCEmails = sessionInfo.CcEmails;
                emailData.Subject = string.IsNullOrEmpty(sessionInfo.Subject) ? $"Session {sessionId} status changed" : sessionInfo.Subject.Replace("{sessionId}", sessionId).Replace("{status}", status);
                emailData.Body = string.IsNullOrEmpty(sessionInfo.Body) ? $"Session {sessionId} status changed to {status} -> {Environment} Environment" : sessionInfo.Body.Replace("{sessionId}", sessionId).Replace("{status}", status);
            }

            Thread thread = new Thread(() =>
            {
                GEmailUtil.SendEmail(emailData);
            });
            thread.Start();

            //emailData.EmailSubject = $"Session {sessionId} status changed";
            //emailData.EmailBody = $"Session {sessionId} status changed to {status} -> {Environment} Environment";

            //Thread thread = new Thread(() =>
            //{
            //    var EmailSubject = $"Session {sessionId} status changed";
            //    var EmailBody = $"Session {sessionId} status changed to {status} -> {Environment} Environment";
            //    GEmailUtil.SendEmail(EmailSubject, EmailBody, null);
            //});
            //thread.Start();

            //SendEmail
            //emailService.SendEmailAsync(emailData);
        }

        public SessionEmails GetSessionAlertConfiguration(string SessionId)
        {
            SessionEmails sessionEmails = null;

            if (!string.IsNullOrEmpty(SessionId))
            {
                using (var context = new FIXMonitorContext())
                {
                    var sessionInfo = context.Sessions.FirstOrDefault(s => s.SessionId == SessionId);

                    if (sessionInfo != null)
                    {
                        sessionEmails = new SessionEmails()
                        {
                            SessionId = sessionInfo.SessionId,
                            ToEmails = sessionInfo.ToEmails,
                            CcEmails = sessionInfo.CcEmails,
                            EmailStatus = sessionInfo.EmailStatus,
                            Timeout = sessionInfo.Timeout,
                            Recurring = sessionInfo.Recurring,
                            Subject = sessionInfo.Subject,
                            Body = sessionInfo.Body,
                            ScheduleTime = sessionInfo.ScheduleTime
                        };

                        return sessionEmails;
                    }

                    return sessionEmails;
                }
            }

            return sessionEmails;
        }

        public bool AddSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            if (sessionEmails != null)
            {
                var sessionConfiguration = new Sessions()
                {
                    SessionId = sessionEmails.SessionId,
                    ToEmails = sessionEmails.ToEmails,
                    CcEmails = sessionEmails.CcEmails,
                    EmailStatus = sessionEmails.EmailStatus,
                    Timeout = sessionEmails.Timeout,
                    Recurring = sessionEmails.Recurring,
                    Subject = sessionEmails.Subject,
                    Body = sessionEmails.Body,
                    ScheduleTime = sessionEmails.ScheduleTime
                };

                using (var context = new FIXMonitorContext())
                {
                    context.Sessions.Add(sessionConfiguration);
                    context.SaveChanges();
                }

                return true;
            }

            return false;
        }

        public bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            if (sessionEmails != null)
            {
                var updatedSessionConfiguration = new Sessions()
                {
                    SessionId = sessionEmails.SessionId,
                    ToEmails = sessionEmails.ToEmails,
                    CcEmails = sessionEmails.CcEmails,
                    EmailStatus = sessionEmails.EmailStatus,
                    Timeout = sessionEmails.Timeout,
                    Recurring = sessionEmails.Recurring,
                    Subject = sessionEmails.Subject,
                    Body = sessionEmails.Body,
                    ScheduleTime = sessionEmails.ScheduleTime
                };

                if (EmailNotifier.emailTimer.ContainsKey(updatedSessionConfiguration.SessionId))
                {
                    System.Timers.Timer timer;
                    EmailNotifier.emailTimer.TryGetValue(updatedSessionConfiguration.SessionId, out timer);
                    timer.Stop();
                    timer.Dispose();

                    EmailNotifier.emailTimer.Remove(updatedSessionConfiguration.SessionId);

                    if (updatedSessionConfiguration.EmailStatus)
                    {
                        int intervalInMilliseconds = TimeConverter.GetTimeInMilliseconds(updatedSessionConfiguration.Timeout);

                        emailNotifier = new EmailNotifier(intervalInMilliseconds, updatedSessionConfiguration.SessionId, "DISCONNECTED", updatedSessionConfiguration);
                        EmailNotifier.emailTimer.Add(updatedSessionConfiguration.SessionId, emailNotifier.getTimerInstance());
                    }
                }

                using (var context = new FIXMonitorContext())
                {
                    context.Sessions.Update(updatedSessionConfiguration);
                    context.SaveChanges();
                }

                return true;
            }

            return false;
        }

        public bool DeleteSessionAlertConfiguration(string SessionId)
        {
            if (!string.IsNullOrEmpty(SessionId))
            {
                if (EmailNotifier.emailTimer.ContainsKey(SessionId))
                {
                    System.Timers.Timer timer;
                    EmailNotifier.emailTimer.TryGetValue(SessionId, out timer);
                    timer.Stop();
                    timer.Dispose();

                    EmailNotifier.emailTimer.Remove(SessionId);
                }

                using (var context = new FIXMonitorContext())
                {
                    var session = context.Sessions.FirstOrDefault(s => s.SessionId == SessionId);

                    if (session != null)
                    {
                        context.Sessions.Remove(session);
                        context.SaveChanges();

                        return true;
                    }

                    return false;
                }
            }

            return false;
        }
    }
}
