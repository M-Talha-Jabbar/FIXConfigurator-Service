using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    interface IJenkinsHandler
    {
        Task<string> JenkinsTrigger(string branchName, string environment);
    }
}
