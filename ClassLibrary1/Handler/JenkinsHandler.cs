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
using Newtonsoft.Json;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
using FIXMonitorBusinessLogicLayer.PollingWorkers;
using System.Threading;
using FIXMonitorBusinessLogicLayer.ConfigSections;
using FIXMonitorBusinessLogicLayer.Data;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class JenkinsHandler : IJenkinsHandler
    {
        private readonly string httpRequest = "http://";
        JenkinsConfigSection jenkinsConfigSection;
        private HttpClient client;
        private string[] crumb_token;
        private PollingWorker pw;
        private JenkinsJobStatus lastBuild;

        private JenkinsHandler()
        {
            client = new HttpClient();
            jenkinsConfigSection = ConfigurationManager.GetSection("JenkinsConfigSection") as JenkinsConfigSection;
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

            $"{jenkinsConfigSection.JenkinsUsername}:{jenkinsConfigSection.JenkinsPassword}")));

            var requestMessage = new HttpRequestMessage(HttpMethod.Get, jenkinsConfigSection.JenkinsCrumbUrl);

            var task = await client.SendAsync(requestMessage);

            var content = await task.Content.ReadAsStringAsync();

            var token = content.Split(':');

            return token;
        }
        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            try
            {
                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkinsConfigSection.JenkinsJobTriggerUrl}?Branch={branchName}&Environment={environment}");

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
                var isJenkinsAgentOnlineRes = await isJenkinsAgentOnline(AgentName);
                if (!isJenkinsAgentOnlineRes) return $"Jenkins Node {AgentName} is Offline";

                DeploymentPath = System.Web.HttpUtility.UrlEncode(DeploymentPath);

                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkinsConfigSection.JenkinsJobTriggerUrl}?Branch={branchName}&Environment={environment}&DeploymentPath={DeploymentPath}&AgentName={AgentName}");

                jenkins_job_trigger.Headers.Add(crumb_token[0], crumb_token[1]);

                lastBuild = JenkinsLatestJobStatus();

                var triggerStatus = await client.SendAsync(jenkins_job_trigger);

                var status_code = triggerStatus.StatusCode.ToString();

                pw = new PollingWorker(SendJenkinsJobStatusToClient, AfterPolling, jenkinsConfigSection.JenkinsJobStatusTimeoutSeconds, jenkinsConfigSection.JenkinsJobStatusIntervalSeconds);
                pw.Poll();

                return status_code;

            }
            catch (Exception e)
            {
                return e.Message;
            }
        }

        public async Task<string> StartFixEngine(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            try
            {
                var isJenkinsAgentOnlineRes = await isJenkinsAgentOnline(fixEngineJenkinsConfiguration.JenkinsAgentName);
                if (!isJenkinsAgentOnlineRes) return $"Jenkins Node {fixEngineJenkinsConfiguration.JenkinsAgentName} is Offline";

                var scriptPath = System.Web.HttpUtility.UrlEncode(fixEngineJenkinsConfiguration.Path);

                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkinsConfigSection.JenkinsJobStartNStopFixEngineUrl}?ScriptPath={scriptPath}&ScriptName={jenkinsConfigSection.JenkinsJobStartFixEngineScript}&EngineIP={fixEngineJenkinsConfiguration.EngineIp}&AgentName={fixEngineJenkinsConfiguration.JenkinsAgentName}&Username={fixEngineJenkinsConfiguration.FixEngineMachineUsername}&Password={fixEngineJenkinsConfiguration.FixEngineMachinePassword}");

                jenkins_job_trigger.Headers.Add(crumb_token[0], crumb_token[1]);

                lastBuild = JenkinsLatestJobStatus();

                var triggerStatus = await client.SendAsync(jenkins_job_trigger);

                var status_code = triggerStatus.StatusCode.ToString();

                return status_code;
            }
            catch(Exception e)
            {
                return e.Message;
            }
        }

        public async Task<string> StopFixEngine(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            try
            {
                var isJenkinsAgentOnlineRes = await isJenkinsAgentOnline(fixEngineJenkinsConfiguration.JenkinsAgentName);
                if (!isJenkinsAgentOnlineRes) return $"Jenkins Node {fixEngineJenkinsConfiguration.JenkinsAgentName} is Offline";

                var scriptPath = System.Web.HttpUtility.UrlEncode(fixEngineJenkinsConfiguration.Path);

                var jenkins_job_trigger = new HttpRequestMessage(HttpMethod.Post, $"{jenkinsConfigSection.JenkinsJobStartNStopFixEngineUrl}?Script_Path={scriptPath}&Script_Name={jenkinsConfigSection.JenkinsJobStopFixEngineScript}&EngineIP={fixEngineJenkinsConfiguration.EngineIp}&AgentName={fixEngineJenkinsConfiguration.JenkinsAgentName}&UserName={fixEngineJenkinsConfiguration.FixEngineMachineUsername}&Password={fixEngineJenkinsConfiguration.FixEngineMachinePassword}");

                jenkins_job_trigger.Headers.Add(crumb_token[0], crumb_token[1]);

                lastBuild = JenkinsLatestJobStatus();

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
                var JenkinsAgentApi = httpRequest + jenkinsConfigSection.JenkinsMasterNodeDomain + jenkinsConfigSection.JenkinsAgentApi;
                var res = new HttpRequestMessage(HttpMethod.Get, JenkinsAgentApi);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = await client.SendAsync(res);
                var content = await response.Content.ReadAsStringAsync();
                var AgentsInfo = JsonConvert.DeserializeObject<JenkinsAgentResponse>(content);
                var agents = new List<string>();
                foreach (var AgentInfo in AgentsInfo.computer)
                {
                    agents.Add(AgentInfo.displayName);
                }

                return agents;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cant fetch jenkins agent list {ex.Message}");
                return new List<string>();
            }

        }

        private async Task<bool> isJenkinsAgentOnline(string jenkinsAgentName)
        {
            try
            {
                var api = httpRequest + jenkinsConfigSection.JenkinsMasterNodeDomain + jenkinsConfigSection.JenkinsAgentInfoApi.Replace("agentName", jenkinsAgentName);
                var res = new HttpRequestMessage(HttpMethod.Get, api);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = await client.SendAsync(res);
                var content = await response.Content.ReadAsStringAsync();
                var AgentInfo = JsonConvert.DeserializeObject<JenkinsAgentInfo>(content);
                return !AgentInfo.offline;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cant fetch jenkins agents online status {ex.Message}");
                return false;
            }
        }

        public bool AbortJenkinsLastJob()
        {
            var methodName = "AbortJenkinsLastJob";
            Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has started");
            try
            {
                var api = httpRequest + jenkinsConfigSection.JenkinsMasterNodeDomain + jenkinsConfigSection.JenkinsLastJobAbortApi;
                var res = new HttpRequestMessage(HttpMethod.Post, api);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = client.SendAsync(res).Result;
                var status_code = response.IsSuccessStatusCode;
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName}: request successfull with {status_code}");
                return status_code; // 200 if success
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName}: request failed {ex.Message}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has completed");
            }
        }

        public JenkinsJobStatus JenkinsLatestJobStatus() {
            var methodName = "JenkinsLatestJobStatus";
            Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has started");
            try
            {
                var api = httpRequest + jenkinsConfigSection.JenkinsMasterNodeDomain + jenkinsConfigSection.JenkinsLatestJobInfo;
                var res = new HttpRequestMessage(HttpMethod.Get, api);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = client.SendAsync(res).Result;
                var content = response.Content.ReadAsStringAsync().Result;
                var jenkinsJobStatus = JsonConvert.DeserializeObject<JenkinsJobStatus>(content);
                return jenkinsJobStatus;
            }
            catch(Exception ex) 
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName}: request failed {ex.Message}");
                return null;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has completed");
            }
        }

        public async Task<JenkinsJobStatus>  JenkinsLatestJobStatusAsync()
        {
            var methodName = "JenkinsLatestJobStatus";
            Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has started");
            try
            {
                var api = httpRequest + jenkinsConfigSection.JenkinsMasterNodeDomain + jenkinsConfigSection.JenkinsLatestJobInfo;
                var res = new HttpRequestMessage(HttpMethod.Get, api);
                res.Headers.Add(crumb_token[0], crumb_token[1]);
                var response = client.SendAsync(res).Result;
                var content = await response.Content.ReadAsStringAsync();
                var jenkinsJobStatus = JsonConvert.DeserializeObject<JenkinsJobStatus>(content);
                return jenkinsJobStatus;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName}: request failed {ex.Message}");
                return null;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method {methodName} in JenkinsHandler has completed");
            }
        }

        public void SendJenkinsJobStatusToClient() 
        {
            var res = JenkinsLatestJobStatus();

            Logging.LogMessage(LOGTYPE.Info, $"{res.id} {lastBuild.id}");

            if (res.id == lastBuild.id) return; 

            Observable observable = new Observable();

            Logging.LogMessage(LOGTYPE.Info, $"{res.inProgress} {res.result}");

            observable.SendJenkinsJobStatus(res);

            if (!res.inProgress && pw.isPolling()) 
            {
                pw.Stop();
            }
             
        }

        public void AfterPolling() 
        {
            AbortJenkinsLastJob();
            SendJenkinsJobStatusToClient();
        }

        
    }
}
