using CoreLogging;
using FIXMonitorBusinessLogicLayer.Data;
using ef = Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.DataAccess.IRepository;

namespace FIXMonitorBusinessLogicLayer.DataAccess.Repositories
{
    public class JenkinsRepositiory : IJenkinsRepository
    {
        // jenkins config create
        public async Task<FixEngineJenkinsConfiguration> CreateJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: CreateJenkinsConfigAsync in JenkinsRepository started");

            try
            {
                if (fixEngineJenkinsConfiguration == null) return null;
                    await fixMonitorContext.FixEngineJenkinsConfiguration.AddAsync(fixEngineJenkinsConfiguration);
                    Logging.LogMessage(LOGTYPE.Info, $"JenkinsConfig added successfully");
                    return fixEngineJenkinsConfiguration;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"JenkinsConfig cannot be added {ex.Message}");
                return null;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: CreateJenkinsConfigAsync ended");
            }
        }


        // jenkins config update
        public FixEngineJenkinsConfiguration UpdateJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext)
        {
            Logging.LogMessage(LOGTYPE.Info, $"Method name: UpdateJenkinsConfigAsync in JenkinsRepository started");

            try
            {
                if (fixEngineJenkinsConfiguration == null) return null;

                    var res = fixMonitorContext.FixEngineJenkinsConfiguration.Update(fixEngineJenkinsConfiguration);
                    var values = res.CurrentValues;
                    Logging.LogMessage(LOGTYPE.Info, $"Updated successfully");
                    return fixEngineJenkinsConfiguration;
                
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"changes Updated successfully {ex.Message} {ex.StackTrace}");
                return null;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: UpdateJenkinsConfigAsync ended");
            }
        }

        // jenkins config read
        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfigAsync(string engineID, FIXMonitorContext fixMonitorContext)
        {
            Logging.LogMessage(LOGTYPE.Info, $"Method name: GetJenkinsConfigAsync in JenkinsRepository started");
            FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration = null;
            try
            {
                if (string.IsNullOrEmpty(engineID)) return fixEngineJenkinsConfiguration;
                    fixEngineJenkinsConfiguration = await fixMonitorContext.FixEngineJenkinsConfiguration.FirstOrDefaultAsync(x => x.EngineId == engineID);

                    Logging.LogMessage(LOGTYPE.Info, $"read configuration succcessfully");
                    return fixEngineJenkinsConfiguration;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cannot read {ex.Message}");
                return fixEngineJenkinsConfiguration;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: GetJenkinsConfigAsync ended");
            }
        }

        // jenkins config delete

        public bool DeleteJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: DeleteJenkinsConfigAsync in JenkinsRepository started");

            try
            {
                var res = fixMonitorContext.FixEngineJenkinsConfiguration.Remove(fixEngineJenkinsConfiguration);
                Logging.LogMessage(LOGTYPE.Info, $"removed jenkins configuration succcessfully");
                return true;
                
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cannot delete configuration {ex.Message}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: DeleteJenkinsConfigAsync ended");
            }
        }
    }
}
