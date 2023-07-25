using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Converter;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.Notifier;
using GEmail;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    class EmailHandler : IEmailHandler
    {
        private EmailNotifier emailNotifier;
        public EmailHandler() {}

        public void DispatchEmail(EmailData emailData)
        {
            Task.Run(() => GEmailUtil.SendEmail(emailData));
        }

        public SessionEmails GetSessionAlertConfiguration(string SessionId)
        {
            SessionEmails sessionEmails = null;

            if (!string.IsNullOrEmpty(SessionId))
            {
                using (var context = new FIXMonitorContext())
                {
                    var sessionInfo = context.FixSessions.FirstOrDefault(s => s.SessionId == SessionId);

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
                var sessionConfiguration = new FixSessions()
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
                    context.FixSessions.Add(sessionConfiguration);
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
                var updatedSessionConfiguration = new FixSessions()
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
                        int intervalInMilliseconds = TimeConverterUtility.GetTimeInMilliseconds(updatedSessionConfiguration.Timeout);

                        emailNotifier = new EmailNotifier(intervalInMilliseconds, updatedSessionConfiguration.SessionId, "DISCONNECTED", updatedSessionConfiguration);
                        EmailNotifier.emailTimer.Add(updatedSessionConfiguration.SessionId, emailNotifier.getTimerInstance());
                    }
                }

                using (var context = new FIXMonitorContext())
                {
                    context.FixSessions.Update(updatedSessionConfiguration);
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
                    var session = context.FixSessions.FirstOrDefault(s => s.SessionId == SessionId);

                    if (session != null)
                    {
                        context.FixSessions.Remove(session);
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
