using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EmailSender;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    class EmailHandler
    {
        EmailService emailService;
        private readonly string FromEmail = System.Configuration.ConfigurationManager.AppSettings["FromEmail"].ToString();
        private readonly string CommaSeperatedToEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedToEmails"].ToString();
        private readonly string CommaSeperatedCCEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedCCEmails"].ToString();
        private readonly string EmailApiKey = System.Configuration.ConfigurationManager.AppSettings["EmailApiKey"].ToString();
        private readonly string Environment = System.Configuration.ConfigurationManager.AppSettings["Environment"].ToString();
        public EmailHandler()
        {
            emailService = new EmailService(EmailApiKey);
        }

        public void SendEmail(string sessionId, string status)
        {
            EmailData emailData = new EmailData()
            {
                FromEmail = FromEmail,
                CommaSeperatedToEmails = CommaSeperatedToEmails,
                CommaSeperatedCCEmails = CommaSeperatedCCEmails,
                IsBodyHtml = false
            };

            emailData.EmailSubject = $"Session {sessionId} status changed";
            emailData.EmailBody = $"Session {sessionId} status changed to {status} -> {Environment} Environment";

            //SendEmail
            //emailService.SendEmailAsync(emailData).
        }
    }
}
