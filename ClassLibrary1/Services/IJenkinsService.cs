using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Services
{
    public interface IJenkinsService
    {
        Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration);
        Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration);
        Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string FixEngineIpAndPort);
        Task<bool> DeleteJenkinsConfiguration(string FixEngineIpAndPort);
        Task<string> JenkinsTrigger(string branchName, string environment);
        Task<string> JenkinsTrigger(string branchName, string environment, string FixEngineIpAndPort);
        Task<IEnumerable<string>> GetJenkinsSlaveNodes();
        Task<JenkinsJobStatus> GetJenkinsLatestJobStatus();
    }
}
