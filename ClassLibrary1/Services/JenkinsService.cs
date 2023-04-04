using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.Repositories;
using System.Collections.Concurrent;
using CoreLogging;
using System.Reflection;
using Microsoft.AspNetCore.Mvc;
using DevExtreme.AspNet.Data;
using System.Configuration;
using System.Net.Http;
using System.Net.Http.Headers;

namespace FIXMonitorBusinessLogicLayer.Services
{
    public class JenkinsService
    {
        public ConcurrentDictionary<string, FixEngineJenkinsConfiguration> FixEngineJenkinsConfigurations { get; private set; }
        public JenkinsRepositiory JenkinsRepositiory { get; private set; }

        public JenkinsService() {
            FixEngineJenkinsConfigurations = new ConcurrentDictionary<string, FixEngineJenkinsConfiguration>();
            JenkinsRepositiory = new JenkinsRepositiory();
        }

        public async Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            var res = await JenkinsRepositiory.CreateJenkinsConfigAsync(fixEngineJenkinsConfiguration);

            if (res != null) {
                FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                return true;
            }

            return false;
        }

        public async Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            var res = await JenkinsRepositiory.UpdateJenkinsConfigAsync(fixEngineJenkinsConfiguration);

            if (res != null)
            {
                FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                return true;
            }

            return false;
        }

        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string FixEngineIpAndPort)
        {
            FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration;
            var res = FixEngineJenkinsConfigurations.TryGetValue(FixEngineIpAndPort, out fixEngineJenkinsConfiguration);
            if (res)
            {
                return fixEngineJenkinsConfiguration;
            }
            fixEngineJenkinsConfiguration = await JenkinsRepositiory.GetJenkinsConfigAsync(FixEngineIpAndPort);

            return fixEngineJenkinsConfiguration; // possibly null
        }

        public async Task<bool> DeleteJenkinsConfiguration(string FixEngineIpAndPort)
        {
            FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration;
            try
            {
                var res = FixEngineJenkinsConfigurations.TryGetValue(FixEngineIpAndPort, out fixEngineJenkinsConfiguration);
                bool isRemovedFromCache = true;
                bool isRemovedFromDB = true;
                if (res)
                {
                    isRemovedFromCache = FixEngineJenkinsConfigurations.TryRemove(FixEngineIpAndPort, out fixEngineJenkinsConfiguration);
                }
                else if (!res)
                {
                    fixEngineJenkinsConfiguration = await JenkinsRepositiory.GetJenkinsConfigAsync(FixEngineIpAndPort);
                }

                isRemovedFromDB = await JenkinsRepositiory.DeleteJenkinsConfigAsync(fixEngineJenkinsConfiguration);

                if (isRemovedFromCache && isRemovedFromDB) return true;
                
                return false;
            }
            catch (Exception ex) {

                Logging.LogMessage(LOGTYPE.Error, $"cannot delete configuration {ex.StackTrace}");

                return false;
            }
          }

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
