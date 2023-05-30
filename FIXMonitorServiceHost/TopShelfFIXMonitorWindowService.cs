using FIXMonitorBusinessLogicLayer;
using System;
using System.Configuration;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Description;
using Topshelf;

namespace FIXMonitorServiceHost
{
    class TopShelfFIXMonitorWindowService : ServiceControl
    {
        public ServiceHost serviceHost = null;
        public bool Start(HostControl hostControl)
        {
            if (serviceHost != null)
            {
                serviceHost.Close();
            }

            string address = ConfigurationManager.AppSettings["baseAddress"].ToString();
            Uri baseAddress = new Uri(address);

            serviceHost = new ServiceHost(typeof(FIXMonitorService.FIXMonitorService));
            ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
            smb.HttpGetEnabled = true;
            smb.MetadataExporter.PolicyVersion = PolicyVersion.Policy15;
            //serviceHost.Description.Behaviors.Add(smb);

            serviceHost.Open();

            FIXMonitorDataCache.GetFIXMonitorDataCacheInstance();

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            if (serviceHost != null)
            {
                serviceHost.Close();
                serviceHost = null;
            }
            return true;
        }
    }
}
