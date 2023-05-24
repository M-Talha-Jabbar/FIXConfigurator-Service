using FIXMonitorBusinessLogicLayer.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataAccess.IRepository
{
    public interface IJenkinsRepository
    {
        Task<FixEngineJenkinsConfiguration> CreateJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext);
        FixEngineJenkinsConfiguration UpdateJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext);
        Task<FixEngineJenkinsConfiguration> GetJenkinsConfigAsync(string engineID, FIXMonitorContext fixMonitorContext);
        bool DeleteJenkinsConfigAsync(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration, FIXMonitorContext fixMonitorContext);
    }
}
