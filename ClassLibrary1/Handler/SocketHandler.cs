using CoreLogging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reactive;
using System.Reactive.Subjects;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class SocketHandler
    {
        //static int heartbeat = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["heartbeatIntervalForFixHub"].ToString());
        private int port;
        private string hostname;
        private string fixEngineName;
        private bool isRunning = false;
        private static int waitBeforeConnecting = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["waitBeforeConnecting"].ToString());
        private BehaviorSubject<bool> subject;

        public SocketHandler(string hostname, int port, string fixEngineName)
        {
            this.hostname = hostname;
            this.port = port;
            this.fixEngineName = fixEngineName;
            subject = new BehaviorSubject<bool>(isRunning);
        }

        public void CheckPortStatus()
        {
            while (true)
            {
                bool isPortOpen = IsPortOpen();
                string portStatus = isPortOpen ? "Open" : "Closed";
                Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} Port {port} is: {portStatus}");

                if(isPortOpen && !isRunning)
                {
                    isRunning = true;
                    subject.OnNext(isRunning);
                    Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} on {hostname}:{port} is running");
                }
                else if(!isPortOpen)
                {
                    isRunning = false;
                    subject.OnNext(isRunning);
                    Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} on {hostname}:{port} is NOT running");
                }

                Thread.Sleep(waitBeforeConnecting);
            }
        }

        private bool IsPortOpen()
        {
            try
            {
                using (TcpClient tcpClient = new TcpClient())
                {
                    tcpClient.Connect(hostname, port);
                    return true;
                }
            }
            catch (SocketException)
            {
                return false;
            }
        }

        public IObservable<bool> GetStatus()
        {
            return subject;
        }
    }
}
