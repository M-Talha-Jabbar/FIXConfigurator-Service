using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace GEmail
{
    public class GEmailUtil
    {
        private static SmtpClient _client;
        private static MailMessage _mail;

        private static object _isSendingMail = 1;

        private static string stEmailUser = System.Configuration.ConfigurationManager.AppSettings["FromEmail"];
        private static string stEmailPass = System.Configuration.ConfigurationManager.AppSettings["FromPass"];

        private static void InitializeSmtpClient()
        {
            SmtpClient client = new SmtpClient("smtp-mail.outlook.com");
            client.Port = 587;
            client.DeliveryMethod = SmtpDeliveryMethod.Network;
            client.UseDefaultCredentials = false;

            System.Net.NetworkCredential credentials =
                new System.Net.NetworkCredential(stEmailUser, stEmailPass);
            client.EnableSsl = true;
            client.Credentials = credentials;

            _client = client;
        }
        public static void ConfigureMail()
        {
            string EmailTo = System.Configuration.ConfigurationManager.AppSettings["CommaSeperatedToEmails"];

            InitializeSmtpClient();

            MailMessage mail = new MailMessage();
            mail.From = new MailAddress(stEmailUser);

            var SplitEmail = EmailTo.Split(',');

            foreach (var email in SplitEmail)
            {
                mail.To.Add(email);
            }

            _mail = mail;
        }

        public static void SendEmail(string stSubject, string stBody, string[] stFileNames)
        {
            try
            {
                var timeUtc = DateTime.UtcNow;
                TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                _mail.Subject = stSubject + " " + easternTime.ToLongDateString();
                _mail.Body = stBody;

                System.Net.Mail.Attachment attachment;

                if (stFileNames != null)
                {
                    foreach (var stFileName in stFileNames)
                    {
                        attachment = new System.Net.Mail.Attachment(stFileName);
                        _mail.Attachments.Add(attachment);
                    }
                }

                Console.WriteLine("Sending email");
                lock (_isSendingMail)
                {
                    _client.Send(_mail);
                    Console.WriteLine("mail sent");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        public static void SendEmail(EmailData emailData)
        {
            InitializeSmtpClient();

            MailMessage mail = new MailMessage();
            mail.From = new MailAddress(stEmailUser);

            if(!string.IsNullOrEmpty(emailData.CommaSeperatedToEmails))
            {
                var SplitToEmails = emailData.CommaSeperatedToEmails.Split(',');

                foreach (var email in SplitToEmails)
                {
                    mail.To.Add(email);
                }
            }

            if(!string.IsNullOrEmpty(emailData.CommaSeperatedCCEmails))
            {
                var SplitCCEmails = emailData.CommaSeperatedCCEmails.Split(',');

                foreach (var email in SplitCCEmails)
                {
                    mail.CC.Add(email);
                }
            }

            try
            {
                var timeUtc = DateTime.UtcNow;
                TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                DateTime easternTime = TimeZoneInfo.ConvertTimeFromUtc(timeUtc, easternZone);

                mail.Subject = emailData.Subject + " " + easternTime.ToLongDateString();
                mail.Body = emailData.Body;

                Console.WriteLine("Sending email");
                lock (_isSendingMail)
                {
                    _client.Send(mail);
                    Console.WriteLine("mail sent");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
