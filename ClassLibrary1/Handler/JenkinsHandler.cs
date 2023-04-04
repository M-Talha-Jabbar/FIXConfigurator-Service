using Microsoft.Extensions.Configuration;
using System.Configuration;
using System.Net.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http.Headers;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.Services;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class JenkinsHandler: IJenkinsHandler
    {
        public JenkinsService _JenkinsService { get; private set; }
        public JenkinsHandler(JenkinsService JenkinsService)
        {
            _JenkinsService = JenkinsService;
        }
        public async Task<string> JenkinsTrigger(string branchName, string environment)
        {
            return "false";
        }

    }
}
