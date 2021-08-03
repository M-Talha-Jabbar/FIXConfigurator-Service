using System;
using System.Configuration;
using System.ServiceModel;
using FIXMonitorBusinessLogicLayer;

namespace FIXMonitorServiceHost
{
    class Program
    {
        static void Main(string[] args)
        {
            FIXMonitorDataCacheWrapper.GetInstance();
            string address = ConfigurationManager.AppSettings["baseAddress"].ToString();
            Uri baseAddress = new Uri(address);

            // Create the ServiceHost.
            using (ServiceHost host = new ServiceHost(typeof(FIXMonitorService.FIXMonitorService)))
            {
                host.Open();
                // Enable metadata publishing.
                //ServiceMetadataBehavior smb = new ServiceMetadataBehavior();
                //smb.HttpGetEnabled = true;
                //smb.MetadataExporter.PolicyVersion = PolicyVersion.Policy15;
                //host.Description.Behaviors.Add(smb);

                //// Open the ServiceHost to start listening for messages. Since
                //// no endpoints are explicitly configured, the runtime will create
                //// one endpoint per base address for each service contract implemented
                //// by the service.
                //host.Open();

                Console.WriteLine("The service is ready at {0}", baseAddress);
                Console.WriteLine("Press <Enter> to stop the service.");
                Console.ReadLine();

                // Close the ServiceHost.
                host.Close();
            }
        }
    }
}
