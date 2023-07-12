using System;
using System.Collections.Generic;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace FIXMonitorBusinessLogicLayer.Data
{
    public class FixEngineJenkinsConfiguration
    {
        public string Path { get; set; }
        public string JenkinsAgentName { get; set; }
        public string EngineId { get; set; }
        public string FixEngineMachinePassword { get; set; }
        public string FixEngineMachineUsername { get; set; }
        public string EngineIp { get; set; }

        public FixEngineJenkinsConfiguration GetClone()
        {
            FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration = (FixEngineJenkinsConfiguration)this.MemberwiseClone();
            return fixEngineJenkinsConfiguration;
        }
    }
}
