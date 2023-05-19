using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Data;
using System.Collections.Concurrent;
using CoreLogging;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.DataAccess.Repositories;
using FIXMonitorBusinessLogicLayer.DataAccess.IUnitOfWork;
using FIXMonitorBusinessLogicLayer.DataAccess.UnitOfWork;
using FIXMonitorBusinessLogicLayer.DataAccess.IRepository;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;

namespace FIXMonitorBusinessLogicLayer.Services
{
    public class JenkinsService : IJenkinsService
    {
        public ConcurrentDictionary<string, FixEngineJenkinsConfiguration> FixEngineJenkinsConfigurations { get; private set; }
        public IJenkinsRepository JenkinsRepositiory { get; private set; }

        public JenkinsService() {
            FixEngineJenkinsConfigurations = new ConcurrentDictionary<string, FixEngineJenkinsConfiguration>();
            JenkinsRepositiory = new JenkinsRepositiory();
        }

        public async Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            if (fixEngineJenkinsConfiguration == null) return false;

            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
            {
                bool savechanges = false;
                try
                {
                    var res = await JenkinsRepositiory.CreateJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context);
                     savechanges = await JenkinsUnitOfWork.SaveAsync();
                    if(savechanges)
                        FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                   
                    return savechanges;
                }
                catch (Exception ex)
                {
                    Logging.LogMessage(LOGTYPE.Error, $"Method AddJenkinsConfiguration in Jenkins Service {ex.Message}");
                }
                return savechanges;
            }
        }

        public async Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            if (fixEngineJenkinsConfiguration == null) return false;

            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
            {
                bool savechanges = false;
                try
                {
                    var res = JenkinsRepositiory.UpdateJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context);

                    savechanges = await JenkinsUnitOfWork.SaveAsync();

                    if (savechanges)
                    {
                        FixEngineJenkinsConfiguration oldValue;
                        if (FixEngineJenkinsConfigurations.ContainsKey(fixEngineJenkinsConfiguration.FixEngineIpAndPort))
                        {
                            FixEngineJenkinsConfigurations.TryGetValue(fixEngineJenkinsConfiguration.FixEngineIpAndPort, out oldValue);
                            FixEngineJenkinsConfigurations.TryUpdate(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration, oldValue);
                        }
                        else 
                        {
                            FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                        }
                           
                         return savechanges;
                        }
                    }
                    catch (Exception ex)
                    {
                        Logging.LogMessage(LOGTYPE.Error, $"Method UpdateJenkinsConfiguration in Jenkins Service {ex.Message}");
                    }
                return savechanges;
            }
        }

        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string FixEngineIpAndPort)
        {
            if (FixEngineIpAndPort == null) return null;

            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
            {
                FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration = null;

                try
                {
                    var res = FixEngineJenkinsConfigurations.TryGetValue(FixEngineIpAndPort, out fixEngineJenkinsConfiguration);
                    if (res)
                    {
                        return fixEngineJenkinsConfiguration;
                    }

                    fixEngineJenkinsConfiguration = await JenkinsRepositiory.GetJenkinsConfigAsync(FixEngineIpAndPort, JenkinsUnitOfWork.Context);

                    if (fixEngineJenkinsConfiguration != null &&
                       !FixEngineJenkinsConfigurations.ContainsKey(fixEngineJenkinsConfiguration.FixEngineIpAndPort))
                    {
                        FixEngineJenkinsConfigurations.TryAdd(FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                    }
                }
                catch (Exception ex)
                {
                    Logging.LogMessage(LOGTYPE.Error, $"Method GetJenkinsConfiguration in Jenkins Service {ex.Message}");
                }

                return fixEngineJenkinsConfiguration; // possibly null
            }
        }

        public async Task<bool> DeleteJenkinsConfiguration(string FixEngineIpAndPort)
        {
            if (FixEngineIpAndPort == null) return false;

            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
                {
                bool savechanges = false;
                FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration;
                FixEngineJenkinsConfiguration removedFixEngineJenkinsConfiguration;
                try
                    {
                    fixEngineJenkinsConfiguration = await JenkinsRepositiory.GetJenkinsConfigAsync(FixEngineIpAndPort, JenkinsUnitOfWork.Context);
                    var isRemoved = fixEngineJenkinsConfiguration != null ? JenkinsRepositiory.DeleteJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context) : true;
                   
                    if (isRemoved) 
                    {
                        FixEngineJenkinsConfigurations.TryRemove(FixEngineIpAndPort, out removedFixEngineJenkinsConfiguration);
                    }
                    
                    savechanges = await JenkinsUnitOfWork.SaveAsync();

                    return savechanges;
                    
                    }
                    catch (Exception ex)
                    {
                     Logging.LogMessage(LOGTYPE.Error, $"Method DeleteJenkinsConfiguration in Jenkins Service {ex.Message}");
                     }

                return savechanges;
            }
          }

        public async Task<IEnumerable<string>> GetJenkinsSlaveNodes() 
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            return await _JenkinsHandler.GetJenkinsSlaveNodes();
        }

        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            return await _JenkinsHandler.JenkinsTrigger(branchName, environment, "D:/jenkins_105/workspace/OMSServers/FixHub-Config-Using-Web/Http-Trigger-For-FixHub-Config-Deployment", "Dev_Local");
        }

        public async Task<string> JenkinsTrigger(string branchName, string environment, string FixEngineIpAndPort)
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            var fixEngineJenkinsConfiguration = await GetJenkinsConfiguration(FixEngineIpAndPort);
            

            if (fixEngineJenkinsConfiguration != null)
            {
                var agentName = fixEngineJenkinsConfiguration.JenkinsAgentName;
                var path = fixEngineJenkinsConfiguration.Path;

                if (!string.IsNullOrEmpty(agentName) && !string.IsNullOrEmpty(path)) {
                    return await _JenkinsHandler.JenkinsTrigger(branchName, environment, path, agentName);
                }
            }

            return "Not Created";
        }

        public async Task<string> StartFixEngine(string FixEngineIpAndPort)
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            var fixEngineJenkinsConfiguration = await GetJenkinsConfiguration(FixEngineIpAndPort);

            if(fixEngineJenkinsConfiguration != null)
            {
                var agentName = fixEngineJenkinsConfiguration.JenkinsAgentName;
                var path = fixEngineJenkinsConfiguration.Path;

                if (!string.IsNullOrEmpty(agentName) && !string.IsNullOrEmpty(path))
                {
                    return await _JenkinsHandler.StartFixEngine(path, agentName);
                }
            }

            return "Please fill required field in Jenkins Configuration";
        }

        public async Task<string> StopFixEngine(string FixEngineIpAndPort)
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            var fixEngineJenkinsConfiguration = await GetJenkinsConfiguration(FixEngineIpAndPort);

            if (fixEngineJenkinsConfiguration != null)
            {
                var agentName = fixEngineJenkinsConfiguration.JenkinsAgentName;
                var path = fixEngineJenkinsConfiguration.Path;

                if (!string.IsNullOrEmpty(agentName) && !string.IsNullOrEmpty(path))
                {
                    return await _JenkinsHandler.StopFixEngine(path, agentName);
                }
            }

            return "Please fill required field in Jenkins Configuration";
        }

        public async Task<JenkinsJobStatus> GetJenkinsLatestJobStatus() 
        {
            IJenkinsHandler _JenkinsHandler = await JenkinsHandler.GetInstance();
            return await _JenkinsHandler.JenkinsLatestJobStatusAsync();
        }
    }
}
