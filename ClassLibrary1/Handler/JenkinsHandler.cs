using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Net.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http.Headers;
using FIXMonitorBusinessLogicLayer.IHandler;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class JenkinsHandler: IJenkinsHandler
    {
        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            var jenkins_username = ConfigurationManager.AppSettings["JenkinsUsername"].ToString();
            var jenkins_password = ConfigurationManager.AppSettings["JenkinsPassword"].ToString();
            var jenkins_crumb_url = ConfigurationManager.AppSettings["JenkinsCrumbUrl"].ToString();
            var jenkins_job_trigger_url = ConfigurationManager.AppSettings["JenkinsJobTriggerUrl"].ToString();

            try
            {
                HttpClient client = new HttpClient();

                client.DefaultRequestHeaders.Accept.Clear();

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(

                $"{jenkins_username}:{jenkins_password}")));

                var requestMessage = new HttpRequestMessage(HttpMethod.Get, jenkins_crumb_url);

                var task = await client.SendAsync(requestMessage);

                var content = await task.Content.ReadAsStringAsync();

                var token = content.Split(':');

                var jenkins_crumb = token[1];


                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkins_job_trigger_url}?Branch={branchName}&Environment={environment}");

                jenkins_job_trigger.Headers.Add(token[0], token[1]);

                var triggerStatus = await client.SendAsync(jenkins_job_trigger);

                var status_code = triggerStatus.StatusCode.ToString();
                
                return status_code;

            }
            catch (Exception e)
            {
                return e.Message;
            }
        }

    }
}
