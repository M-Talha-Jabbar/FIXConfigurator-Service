using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace FIXMonitorBusinessLogicLayer.ConfigSections
{
    public class JenkinsConfigSection : ConfigurationSection
    {

        [ConfigurationProperty("JenkinsUsername", IsRequired = true)]
        public string JenkinsUsername
        {
            get { return (string)this["JenkinsUsername"]; }
            set { this["JenkinsUsername"] = value; }
        }

        [ConfigurationProperty("JenkinsPassword", IsRequired = true)]
        public string JenkinsPassword
        {
            get { return (string)this["JenkinsPassword"]; }
            set { this["JenkinsPassword"] = value; }
        }

        [ConfigurationProperty("JenkinsCrumbUrl", IsRequired = true)]
        public string JenkinsCrumbUrl
        {
            get { return (string)this["JenkinsCrumbUrl"]; }
            set { this["JenkinsCrumbUrl"] = value; }
        }

        [ConfigurationProperty("JenkinsJobTriggerUrl", IsRequired = true)]
        public string JenkinsJobTriggerUrl
        {
            get { return (string)this["JenkinsJobTriggerUrl"]; }
            set { this["JenkinsJobTriggerUrl"] = value; }
        }

        [ConfigurationProperty("JenkinsJobStartNStopFixEngineUrl", IsRequired = true)]
        public string JenkinsJobStartNStopFixEngineUrl
        {
            get { return (string)this["JenkinsJobStartNStopFixEngineUrl"]; }
            set { this["JenkinsJobStartNStopFixEngineUrl"] = value; }
        }

        [ConfigurationProperty("JenkinsJobStartFixEngineScript", IsRequired = true)]
        public string JenkinsJobStartFixEngineScript
        {
            get { return (string)this["JenkinsJobStartFixEngineScript"]; }
            set { this["JenkinsJobStartFixEngineScript"] = value; }
        }

        [ConfigurationProperty("JenkinsJobStopFixEngineScript", IsRequired = true)]
        public string JenkinsJobStopFixEngineScript
        {
            get { return (string)this["JenkinsJobStopFixEngineScript"]; }
            set { this["JenkinsJobStopFixEngineScript"] = value; }
        }

        [ConfigurationProperty("JenkinsMasterNodeDomain", IsRequired = true)]
        public string JenkinsMasterNodeDomain
        {
            get { return (string)this["JenkinsMasterNodeDomain"]; }
            set { this["JenkinsMasterNodeDomain"] = value; }
        }

        [ConfigurationProperty("JenkinsAgentApi", IsRequired = true)]
        public string JenkinsAgentApi
        {
            get { return (string)this["JenkinsAgentApi"]; }
            set { this["JenkinsAgentApi"] = value; }
        }

        [ConfigurationProperty("JenkinsAgentInfoApi", IsRequired = true)]
        public string JenkinsAgentInfoApi
        {
            get { return (string)this["JenkinsAgentInfoApi"]; }
            set { this["JenkinsAgentInfoApi"] = value; }
        }

        [ConfigurationProperty("JenkinsLastJobAbortApi", IsRequired = true)]
        public string JenkinsLastJobAbortApi
        {
            get { return (string)this["JenkinsLastJobAbortApi"]; }
            set { this["JenkinsLastJobAbortApi"] = value; }
        }

        [ConfigurationProperty("JenkinsLatestJobInfo", IsRequired = true)]
        public string JenkinsLatestJobInfo
        {
            get { return (string)this["JenkinsLatestJobInfo"]; }
            set { this["JenkinsLatestJobInfo"] = value; }
        }

        [ConfigurationProperty("JenkinsJobStatusTimeoutSeconds", IsRequired = true)]
        public int JenkinsJobStatusTimeoutSeconds
        {
            get { return (int)this["JenkinsJobStatusTimeoutSeconds"]; }
            set { this["JenkinsJobStatusTimeoutSeconds"] = value; }
        }

        [ConfigurationProperty("JenkinsJobStatusIntervalSeconds", IsRequired = true)]
        public int JenkinsJobStatusIntervalSeconds
        {
            get { return (int)this["JenkinsJobStatusIntervalSeconds"]; }
            set { this["JenkinsJobStatusIntervalSeconds"] = value; }
        }

    }
}
