using CoreLogging;
using FIXMonitorBusinessLogicLayer.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Repositories
{
    public class JenkinsRepositiory
    {
        // jenkins config create
        public async Task<bool> createJenkinsConfig(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} in JenkinsRepository started");

            try
            {
                if (fixEngineJenkinsConfiguration == null) return false;

                using (FIXMonitorContext fixMonitorContext = new FIXMonitorContext())
                {
                    await fixMonitorContext.FixEngineJenkinsConfiguration.AddAsync(fixEngineJenkinsConfiguration);
                    await fixMonitorContext.SaveChangesAsync();
                    Logging.LogMessage(LOGTYPE.Info, $"changes saved successfully");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"changes cannot be saved successfully {ex.StackTrace}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} ended");
            }
        }


        // jenkins config update
        public async Task<bool> updateJenkinsConfig(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} in JenkinsRepository started");

            try
            {
                if (fixEngineJenkinsConfiguration == null) return false;

                using (FIXMonitorContext fixMonitorContext = new FIXMonitorContext())
                {
                    fixMonitorContext.FixEngineJenkinsConfiguration.Update(fixEngineJenkinsConfiguration);
                    await fixMonitorContext.SaveChangesAsync();
                    Logging.LogMessage(LOGTYPE.Info, $"changes saved successfully");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"changes cannot be saved successfully {ex.StackTrace}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} ended");
            }
        }

        // jenkins config read
        public async Task<bool> GetJenkinsConfig(string FixEngineIpAndPort)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} in JenkinsRepository started");

            try
            {
                if (!string.IsNullOrEmpty(FixEngineIpAndPort)) return false;

                using (FIXMonitorContext fixMonitorContext = new FIXMonitorContext())
                {
                    var fixEngineJenkinsConfiguration = await fixMonitorContext.FixEngineJenkinsConfiguration.FirstOrDefaultAsync(x => x.FixEngineIpAndPort == FixEngineIpAndPort);

                    Logging.LogMessage(LOGTYPE.Info, $"read configuration succcessfully");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cannot read {ex.StackTrace}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} ended");
            }
        }

        // jenkins config delete

        public async Task<bool> DeleteJenkinsConfig(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {

            Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} in JenkinsRepository started");

            try
            {
                if (fixEngineJenkinsConfiguration != null) return false;

                using (FIXMonitorContext fixMonitorContext = new FIXMonitorContext())
                {
                    fixMonitorContext.FixEngineJenkinsConfiguration.Remove(fixEngineJenkinsConfiguration);
                    await fixMonitorContext.SaveChangesAsync();

                    Logging.LogMessage(LOGTYPE.Info, $"removed jenkins configuration succcessfully");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"cannot delete configuration {ex.StackTrace}");
                return false;
            }
            finally
            {
                Logging.LogMessage(LOGTYPE.Info, $"Method name: {MethodBase.GetCurrentMethod().Name} ended");
            }
        }
    }
}
