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
using CoreLogging;

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
        public string[] crumb_token { get; private set; }

        private JenkinsHandler() {
            client = new HttpClient();
        }

        public static async Task<IJenkinsHandler> GetInstance() 
        {
            var jenkinsHandler = new JenkinsHandler();
            {
                jenkinsHandler.crumb_token = await jenkinsHandler.jenkinsAuthentication();
            }
            return jenkinsHandler;
        }


        private async Task<string[]> jenkinsAuthentication() 
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

        public async Task<string> JenkinsTrigger(string branchName, string environment, string DeploymentPath, string AgentName)
        {

            try
            {
                DeploymentPath = System.Web.HttpUtility.UrlEncode(DeploymentPath);
              
                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkins_job_trigger_url}?Branch={branchName}&Environment={environment}&DeploymentPath={DeploymentPath}&AgentName={AgentName}");

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
            try
            {
                var _jenkinsAgentApi = jenkinsAgentApi.Replace("ip-port", jenkinsMasterNodeDomain);
                var res = new HttpRequestMessage(HttpMethod.Get, _jenkinsAgentApi);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = await client.SendAsync(res);
                var agents = await ParseAgentApiResponse(response);
                return agents;
            }
            catch (Exception ex) {
                Logging.LogMessage(LOGTYPE.Error, $"cant fetch jenkins agent list {ex.Message}");
                return new List<string>();
            }
            
        }

        private async Task<List<string>> ParseAgentApiResponse(HttpResponseMessage response) 
        {
            var content = await response.Content.ReadAsStringAsync();
            var contentarr = content.Split(':');
            List<string> agents = new List<string>();
            for (int i = 0; i < contentarr.Count(); i++)
            {
                if (contentarr[i].Contains("displayName"))
                {
                    agents.Add(contentarr[i + 1].Split(',')[0].Trim('"'));
                }
            }

            return agents;
        } 
           

    }
}
