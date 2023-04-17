using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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

        public void DispatchEmail(EmailData emailData)
        {
            GEmailUtil.SendEmail(emailData);
        }

        public void SendEmail(string sessionId, string status, Sessions sessionInfo)
        {
            EmailData emailData = new EmailData();

            if (sessionInfo == null || string.IsNullOrEmpty(sessionInfo.ToEmails))
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
                
                emailData.Subject = string.IsNullOrEmpty(sessionInfo.Subject) ? $"Session {sessionId} status changed" : Regex.Replace(Regex.Replace(Regex.Replace(sessionInfo.Subject, "{sessionId}", sessionId, RegexOptions.IgnoreCase), "{status}", status, RegexOptions.IgnoreCase), "{environment}", Environment, RegexOptions.IgnoreCase);

                emailData.Body = string.IsNullOrEmpty(sessionInfo.Body) ? $"Session {sessionId} status changed to {status} -> {Environment} Environment" :
                    Regex.Replace(Regex.Replace(Regex.Replace(sessionInfo.Body, "{sessionId}", sessionId, RegexOptions.IgnoreCase), "{status}", status, RegexOptions.IgnoreCase), "{environment}", Environment, RegexOptions.IgnoreCase);
            }

            Task.Run(() => DispatchEmail(emailData));

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

        public void SendEmail(string sessionId, FixTagValues fixTagValues)
        {
            EmailData emailData = new EmailData();

            if (string.IsNullOrEmpty(fixTagValues.ToEmails))
            {
                emailData.CommaSeperatedToEmails = DefaultCommaSeperatedToEmails;
                emailData.CommaSeperatedCCEmails = DefaultCommaSeperatedCCEmails;
                emailData.Subject = $"Session {sessionId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue})";
                emailData.Body = $"Session {sessionId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue}) -> {Environment} Environment";
            }
            else
            {
                emailData.CommaSeperatedToEmails = fixTagValues.ToEmails;
                emailData.CommaSeperatedCCEmails = fixTagValues.CcEmails;
                emailData.Subject = string.IsNullOrEmpty(fixTagValues.Subject) ? $"Session {sessionId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue})" : Regex.Replace(Regex.Replace(Regex.Replace(fixTagValues.Subject, "{sessionId}", sessionId, RegexOptions.IgnoreCase), "{FixTag}", fixTagValues.FixTag, RegexOptions.IgnoreCase), "{FixValue}", fixTagValues.FixValue, RegexOptions.IgnoreCase);
                emailData.Body = string.IsNullOrEmpty(fixTagValues.Body) ? $"Session {sessionId} received a message with Tag/Value ({fixTagValues.FixTag}={fixTagValues.FixValue}) -> {Environment} Environment" : Regex.Replace(Regex.Replace(Regex.Replace(fixTagValues.Body, "{sessionId}", sessionId, RegexOptions.IgnoreCase), "{FixTag}", fixTagValues.FixTag, RegexOptions.IgnoreCase), "{FixValue}", fixTagValues.FixValue, RegexOptions.IgnoreCase);
            }

            Task.Run(() => DispatchEmail(emailData));
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
                };

                if (EmailNotifier.emailTimer.ContainsKey(updatedSessionConfiguration.SessionId))
                {
                    EmailNotifier.DisposeEmailTimer(updatedSessionConfiguration.SessionId);

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
                    EmailNotifier.DisposeEmailTimer(SessionId);

                if (EmailNotifier.recurringEmailsCount.ContainsKey(SessionId))
                    EmailNotifier.recurringEmailsCount.Remove(SessionId);

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

        public List<FixTagValueConfiguration> GetAllFixMessageConfiguration()
        {
            using(var context = new FIXMonitorContext())
            {
                List<FixTagValueConfiguration> allFixTagValueConfigurations = new List<FixTagValueConfiguration>();

                var res = context.FixTagValues.ToList();

                if (res.Count > 0)
                {
                    allFixTagValueConfigurations = res.Select(config => new FixTagValueConfiguration()
                    {
                        Id = config.Id,
                        FixTag = config.FixTag,
                        FixValue = config.FixValue,
                        ToEmails = config.ToEmails,
                        CcEmails = config.CcEmails,
                        EmailStatus = config.EmailStatus,
                        Subject = config.Subject,
                        Body = config.Body,
                        Engine = config.Engine,
                        SessionId = config.SessionId
                    }).ToList();

                    return allFixTagValueConfigurations;
                }

                return allFixTagValueConfigurations;
            }
        }

        public bool AddFixMessageConfiguration(FixTagValueConfiguration fixTagValueConfiguration)
        {
            if(fixTagValueConfiguration != null)
            {
                var newFixTagValueConfiguration = new FixTagValues()
                {
                    FixTag = fixTagValueConfiguration.FixTag,
                    FixValue = fixTagValueConfiguration.FixValue,
                    ToEmails = fixTagValueConfiguration.ToEmails,
                    CcEmails = fixTagValueConfiguration.CcEmails,
                    EmailStatus = fixTagValueConfiguration.EmailStatus,
                    Subject = fixTagValueConfiguration.Subject,
                    Body = fixTagValueConfiguration.Body,
                    Engine = fixTagValueConfiguration.Engine,
                    SessionId = fixTagValueConfiguration.SessionId
                };

                EmailNotifier.fixTagValueConfigurations.Add(newFixTagValueConfiguration);

                using(var context = new FIXMonitorContext())
                {
                    context.FixTagValues.Add(newFixTagValueConfiguration);
                    context.SaveChanges();
                }

                return true;
            }

            return false;
        }

       
        public bool DeleteFixMessageConfiguration(int id)
        {
            using (var context = new FIXMonitorContext())
            {
                var fixTagValueConfiguration = context.FixTagValues.FirstOrDefault(r => r.Id == id);

                if(fixTagValueConfiguration != null)
                {
                    EmailNotifier.fixTagValueConfigurations.Remove(EmailNotifier.fixTagValueConfigurations.FirstOrDefault(r => r.Id == fixTagValueConfiguration.Id));

                    context.FixTagValues.Remove(fixTagValueConfiguration);
                    context.SaveChanges();

                    return true;
                }

                return false;
            }
        }
    }
}
