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
    public class SocketInitiator : IDisposable
    {
        //static int heartbeat = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["heartbeatIntervalForFixHub"].ToString());
        private int port;
        private string hostname;
        private string fixEngineName;
        private bool isRunning = false;
        bool isEngineExist = true;
        private static int waitBeforeConnecting = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["waitBeforeConnecting"].ToString());
        private BehaviorSubject<bool> subject;
        private TcpClient tcpClient;

        public SocketInitiator(string hostname, int port, string fixEngineName)
        {
            this.hostname = hostname;
            this.port = port;
            this.fixEngineName = fixEngineName;
            subject = new BehaviorSubject<bool>(isRunning);
            tcpClient = new TcpClient();
        }

        public async Task CheckPortStatus()
        {
            while (true)
            {
                if (!isEngineExist)
                    break;

                bool isPortOpen = await IsPortOpen();
                string portStatus = isPortOpen ? "Open" : "Closed";
                Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} Port {port} is: {portStatus}");

                if (isPortOpen && !isRunning)
                {
                    isRunning = true;
                    subject.OnNext(isRunning);
                    Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} on {hostname}:{port} is running");
                }
                else if (!isPortOpen)
                {
                    isRunning = false;
                    subject.OnNext(isRunning);
                    Logging.LogMessage(LOGTYPE.Info, $"FixEngine {fixEngineName} on {hostname}:{port} is NOT running");
                }

                await Task.Delay(waitBeforeConnecting);
            }
        }

        private async Task<bool> IsPortOpen()
        {
            try
            {
                if (!tcpClient.Connected)
                {
                    Logging.LogMessage(LOGTYPE.Info, $"Trying to connect with FixEngine {fixEngineName} on {hostname}:{port}");
                    tcpClient.Connect(hostname, port);
                    Logging.LogMessage(LOGTYPE.Info, $"Connected with FixEngine {fixEngineName} on {hostname}:{port}");
                    return true;
                }
                else
                {
                    Logging.LogMessage(LOGTYPE.Info, $"Established Connection with FixEngine {fixEngineName} on {hostname}:{port}");

                    using(NetworkStream stream = tcpClient.GetStream())
                    {
                        byte[] buffer = new byte[256];
                        int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);

                        if (bytesRead == 0)
                        {
                            Logging.LogMessage(LOGTYPE.Info, $"Connection with FixEngine {fixEngineName} on {hostname}:{port} has been closed");
                            DisposeTcpInstanceNClosingTcpConnection();
                            return false;
                        }

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, "Exception: " + ex.Message);
                Logging.LogMessage(LOGTYPE.Error, "StackTrace: " + ex.StackTrace);
                DisposeTcpInstanceNClosingTcpConnection();
                return false;
            }
        }

        private void DisposeTcpInstanceNClosingTcpConnection()
        {
            if (tcpClient != null)
            {
                tcpClient.Close(); // Disposes the TcpClient instance plus closes the TCP connection as well. 
                tcpClient = new TcpClient();
            }
        }

        public IObservable<bool> GetStatus()
        {
            return subject;
        }

        public void Dispose()
        {
            isEngineExist = false;
        }

        ~SocketInitiator()
        {
            Logging.LogMessage(LOGTYPE.Info, $"Disconnecting socket with FixEngine {fixEngineName} on {hostname}:{port}");
            tcpClient.Close();
            Logging.LogMessage(LOGTYPE.Info, $"Socket instance of FixEngine {fixEngineName} on {hostname}:{port} removed");
        }
    }
}
