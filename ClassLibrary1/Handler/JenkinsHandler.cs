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
using FIXMonitorBusinessLogicLayer.Services;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class JenkinsHandler: IJenkinsHandler
    {
        private string jenkins_username = ConfigurationManager.AppSettings["JenkinsUsername"].ToString();
        private string jenkins_password = ConfigurationManager.AppSettings["JenkinsPassword"].ToString();
        private string jenkins_crumb_url = ConfigurationManager.AppSettings["JenkinsCrumbUrl"].ToString();
        private string jenkins_job_trigger_url = ConfigurationManager.AppSettings["JenkinsJobTriggerUrl"].ToString();
        private string jenkinsMasterNodeDomain = ConfigurationManager.AppSettings["JenkinsMasterNodeDomain"].ToString();
        private string jenkinsAgentApi = ConfigurationManager.AppSettings["JenkinsAgentApi"].ToString();
        private HttpClient client;
        private string[] crumbToken;

        public JenkinsHandler() {
            client = new HttpClient();
        }


        public async Task<string[]> jenkinsAuthentication() 
        {
            client.DefaultRequestHeaders.Accept.Clear();

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(

            $"{jenkins_username}:{jenkins_password}")));

            var requestMessage = new HttpRequestMessage(HttpMethod.Get, jenkins_crumb_url);

            var task = await client.SendAsync(requestMessage);

            var content = await task.Content.ReadAsStringAsync();

            var token = content.Split(':');

            return token;
        }
        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            
            try
            {
                var crumb_token = await jenkinsAuthentication();
                
                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkins_job_trigger_url}?Branch={branchName}&Environment={environment}");

                jenkins_job_trigger.Headers.Add(crumb_token[0], crumb_token[1]);

                var triggerStatus = await client.SendAsync(jenkins_job_trigger);

                var status_code = triggerStatus.StatusCode.ToString();

                return status_code;

            }
            catch (Exception e)
            {
                return e.Message;
            }
        }

        public async Task<IEnumerable<string>> GetJenkinsSlaveNodes() 
        {
            var _jenkinsAgentApi = jenkinsAgentApi.Replace("ip-port", jenkinsMasterNodeDomain);
        }


    }
}
