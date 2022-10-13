using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
//using EmailSender;
using FIXMonitorBusinessLogicLayer.Data;
using GEmail;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    class EmailHandler
    {
        //private EmailService emailService;
        //private readonly string FromEmail = System.Configuration.ConfigurationManager.AppSettings["FromEmail"].ToString();
        //private readonly string EmailApiKey = System.Configuration.ConfigurationManager.AppSettings["EmailApiKey"].ToString();

        private readonly string DefaultCommaSeperatedToEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedToEmails"].ToString();
        private readonly string DefaultCommaSeperatedCCEmails = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedCCEmails"].ToString();
        private readonly string Environment = System.Configuration.ConfigurationManager.AppSettings["Environment"].ToString();
        public EmailHandler()
        {
            //emailService = new EmailService(EmailApiKey);
            //GEmailUtil.ConfigureMail();
        }

        public void SendEmail(string sessionId, string status, Sessions sessionInfo)
        {
            EmailData emailData = new EmailData()
            {
                CommaSeperatedToEmails = string.IsNullOrEmpty(sessionInfo.ToEmails) ? DefaultCommaSeperatedToEmails : sessionInfo.ToEmails,
                CommaSeperatedCCEmails = string.IsNullOrEmpty(sessionInfo.CcEmails) ? DefaultCommaSeperatedCCEmails : sessionInfo.CcEmails,
                Subject = $"Session {sessionId} status changed",
                Body = $"Session {sessionId} status changed to {status} -> {Environment} Environment"
            };

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
    }
}
