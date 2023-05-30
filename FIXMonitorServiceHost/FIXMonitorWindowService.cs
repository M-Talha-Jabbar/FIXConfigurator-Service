using FIXMonitorBusinessLogicLayer;
using System;
using System.Configuration;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceProcess;

namespace FIXMonitorServiceHost
{
    class FIXMonitorWindowService : ServiceBase
    {
        public ServiceHost serviceHost = null;
        public FIXMonitorWindowService()
        {
            // Name the Windows Service
            ServiceName = "FIXMonitorWindowsService";
        }

        //public static void Main()
        //{
        //    System.IO.Directory.SetCurrentDirectory(System.AppDomain.CurrentDomain.BaseDirectory);
        //    ServiceBase.Run(new FIXMonitorWindowService());
        //    //  FIXMonitorWindowService service = new FIXMonitorWindowService();
        //    //service.OnStart(null);
        //}

        protected override void OnStart(string[] args)
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
            serviceHost.Description.Behaviors.Add(smb);
            serviceHost.Open();

            FIXMonitorDataCache.GetFIXMonitorDataCacheInstance();

            Console.WriteLine("The service is ready at {0}", baseAddress);
            Console.WriteLine("Press <Enter> to stop the service.");
            Console.ReadLine();
        }

        protected override void OnStop()
        {
            if (serviceHost != null)
            {
                serviceHost.Close();
                serviceHost = null;
            }
        }
    }
}

