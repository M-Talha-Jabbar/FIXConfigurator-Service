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
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.IHandler;

namespace FIXMonitorBusinessLogicLayer.Services
{
    public class JenkinsService : IJenkinsService
    {
        private IJenkinsHandler _JenkinsHandler { get; set; }
        public ConcurrentDictionary<string, FixEngineJenkinsConfiguration> FixEngineJenkinsConfigurations { get; private set; }
        public JenkinsRepositiory JenkinsRepositiory { get; private set; }

        public JenkinsService() {
            FixEngineJenkinsConfigurations = new ConcurrentDictionary<string, FixEngineJenkinsConfiguration>();
            JenkinsRepositiory = new JenkinsRepositiory();
            _JenkinsHandler = new JenkinsHandler();
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
                FixEngineJenkinsConfiguration oldValue;
                bool isUpdated = false;
                if (FixEngineJenkinsConfigurations.ContainsKey(fixEngineJenkinsConfiguration.FixEngineIpAndPort)) 
                {
                    FixEngineJenkinsConfigurations.TryGetValue(fixEngineJenkinsConfiguration.FixEngineIpAndPort, out oldValue);
                    isUpdated = FixEngineJenkinsConfigurations.TryUpdate(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration, oldValue);
                    return isUpdated;
                }
                    
                isUpdated = FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                return isUpdated;
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
                }else if (!res)
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
            return await _JenkinsHandler.JenkinsTrigger(branchName, environment);
        }


    }
}
