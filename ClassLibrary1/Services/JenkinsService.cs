using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Data;
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
using FIXMonitorBusinessLogicLayer.DataAccess.Repositories;
using FIXMonitorBusinessLogicLayer.DataAccess.IUnitOfWork;
using FIXMonitorBusinessLogicLayer.DataAccess.UnitOfWork;
using FIXMonitorBusinessLogicLayer.DataAccess.IRepository;

namespace FIXMonitorBusinessLogicLayer.Services
{
    public class JenkinsService : IJenkinsService
    {
        private IJenkinsHandler _JenkinsHandler { get; set; }
        public ConcurrentDictionary<string, FixEngineJenkinsConfiguration> FixEngineJenkinsConfigurations { get; private set; }
        public IJenkinsRepository JenkinsRepositiory { get; private set; }

        public JenkinsService() {
            FixEngineJenkinsConfigurations = new ConcurrentDictionary<string, FixEngineJenkinsConfiguration>();
            JenkinsRepositiory = new JenkinsRepositiory();
            _JenkinsHandler = new JenkinsHandler();
        }

        public async Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {

            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
            {
                try
                {
                    bool storedInMemory = false;
                    bool storedInDataStore = false;

                    await JenkinsUnitOfWork.CreateTransactionAsync();

                    var res = await JenkinsRepositiory.CreateJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context);

                    if (res != null)
                    {
                        storedInDataStore = true;
                        bool isRemovedFromCache = true;

                        if (FixEngineJenkinsConfigurations.ContainsKey(fixEngineJenkinsConfiguration.FixEngineIpAndPort))
                        {
                            FixEngineJenkinsConfiguration removedFixEngineJenkinsConfiguration;
                            isRemovedFromCache = FixEngineJenkinsConfigurations.TryRemove(fixEngineJenkinsConfiguration.FixEngineIpAndPort, out removedFixEngineJenkinsConfiguration);
                        }
                        storedInMemory = isRemovedFromCache ? 
                            FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration) : 
                            false;
                    }
                    if(storedInMemory && storedInDataStore) return await JenkinsUnitOfWork.CommitAsync();
                    await JenkinsUnitOfWork.RollbackAsync();
                }

                catch (Exception ex)
                {
                    await JenkinsUnitOfWork.RollbackAsync();
                }
                return false;
            }
        }

        public async Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
            {
                try
                {
                    bool updatedInMemory = false;
                    bool upodatedInDataStore = false;

                    await JenkinsUnitOfWork.CreateTransactionAsync();

                    var res = JenkinsRepositiory.UpdateJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context);

                    if (res != null)
                    {
                        upodatedInDataStore = true;
                        FixEngineJenkinsConfiguration oldValue;
                        if (FixEngineJenkinsConfigurations.ContainsKey(fixEngineJenkinsConfiguration.FixEngineIpAndPort))
                        {
                            FixEngineJenkinsConfigurations.TryGetValue(fixEngineJenkinsConfiguration.FixEngineIpAndPort, out oldValue);
                            updatedInMemory = FixEngineJenkinsConfigurations.TryUpdate(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration, oldValue);
                        }
                        else 
                        {
                            updatedInMemory = FixEngineJenkinsConfigurations.TryAdd(fixEngineJenkinsConfiguration.FixEngineIpAndPort, fixEngineJenkinsConfiguration);
                        }

                        if (updatedInMemory && upodatedInDataStore) return await JenkinsUnitOfWork.CommitAsync();
                    }

                    await JenkinsUnitOfWork.RollbackAsync();
                }

                catch (Exception ex)
                {
                    await JenkinsUnitOfWork.RollbackAsync();
                }
                return false;
            }
        }

        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string FixEngineIpAndPort)
        {
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
                    await JenkinsUnitOfWork.RollbackAsync();
                }

                return fixEngineJenkinsConfiguration; // possibly null
            }
        }

        public async Task<bool> DeleteJenkinsConfiguration(string FixEngineIpAndPort)
        {
                using (IUnitOfWork<FIXMonitorContext> JenkinsUnitOfWork = new UnitOfWork<FIXMonitorContext>())
                {
                FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration;
                FixEngineJenkinsConfiguration removedFixEngineJenkinsConfiguration;
                try
                    {
                        await JenkinsUnitOfWork.CreateTransactionAsync();

                        bool isRemovedFromMemory = true;
                        bool isRemovedFromDB = false;

                        var res = FixEngineJenkinsConfigurations.TryGetValue(FixEngineIpAndPort, out fixEngineJenkinsConfiguration);

                    if (res) 
                    {
                        isRemovedFromMemory = FixEngineJenkinsConfigurations.TryRemove(FixEngineIpAndPort, out removedFixEngineJenkinsConfiguration);
                    }
                    
                    fixEngineJenkinsConfiguration = await JenkinsRepositiory.GetJenkinsConfigAsync(FixEngineIpAndPort, JenkinsUnitOfWork.Context);

                    if (fixEngineJenkinsConfiguration != null)
                        isRemovedFromDB = JenkinsRepositiory.DeleteJenkinsConfigAsync(fixEngineJenkinsConfiguration, JenkinsUnitOfWork.Context);

                        if(isRemovedFromMemory && isRemovedFromDB) 
                            return await JenkinsUnitOfWork.CommitAsync();
                        
                        await JenkinsUnitOfWork.RollbackAsync();
                    }
                    catch (Exception ex)
                    {
                        await JenkinsUnitOfWork.RollbackAsync();
                    }

                return false;
            }
            
          }

        public async Task<IEnumerable<string>> GetJenkinsSlaveNodes() 
        {
            return await _JenkinsHandler.GetJenkinsSlaveNodes();
        }

        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            return await _JenkinsHandler.JenkinsTrigger(branchName, environment);
        }


    }
}
