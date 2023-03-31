using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.Repositories;
using System.Collections.Concurrent;

namespace FIXMonitorBusinessLogicLayer.Services
{
    internal class JenkinsService
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

       
    }
}
