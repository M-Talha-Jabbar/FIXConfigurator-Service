using System;
using System.Configuration;
using System.ServiceModel;
using FIXMonitorBusinessLogicLayer;
using Topshelf;

namespace FIXMonitorServiceHost
{
    class Program
    {
        static void Main(string[] args)
        {
            HostFactory.Run(x =>
            {
                x.Service<TopShelfFIXMonitorWindowService>();
                x.EnableServiceRecovery(r => r.RestartService(TimeSpan.FromSeconds(10)));
                x.SetServiceName("FIXMonitorWindowService");
                x.StartAutomatically();
            }
                );
        }
    }
}
