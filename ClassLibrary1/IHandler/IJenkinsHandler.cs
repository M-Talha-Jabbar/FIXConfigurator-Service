using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    public interface IJenkinsHandler
    {
        Task<string> JenkinsTrigger(string branchName, string environment);
        Task<IEnumerable<string>> GetJenkinsSlaveNodes();
    }
}
