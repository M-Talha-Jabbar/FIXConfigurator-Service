using CoreLogging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class SocketListener
    {
        private static int socketListeningPort = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["socketListeningPort"].ToString());
        private static ConcurrentDictionary<string, TcpClient> establishedFixEngineSockets = new ConcurrentDictionary<string, TcpClient>();

        public static async Task ListenClients() // Listening FixEngine Clients
        {
            TcpListener listener = new TcpListener(IPAddress.Any, socketListeningPort);
            listener.Start();
            Logging.LogMessage(LOGTYPE.Info, $"Start Listening to FixEngine Clients");

            while (true)
            {
                TcpClient tcpClient = await listener.AcceptTcpClientAsync();
                Logging.LogMessage(LOGTYPE.Info, $"New FixEngine connected");

                HandleClient(tcpClient);
            }
        }

        private static async Task HandleClient(TcpClient tcpClient) 
        {
            NetworkStream stream = tcpClient.GetStream();
            string fixEngineId = null;

            try
            {
                while (true)
                {
                    byte[] buffer = new byte[256];
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                    fixEngineId = System.Text.Encoding.ASCII.GetString(buffer, 0, bytesRead);

                    // what if any other type of data comes apart from fixengineId

                    establishedFixEngineSockets.TryAdd(fixEngineId, tcpClient);

                    //if (bytesRead == 0)
                    //{
                    //    DisposeTcpInstanceNClosingTcpConnection(tcpClient);
                    //    break;
                    //}
                }
            }
            catch(Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, "Exception: " + ex.Message);
                Logging.LogMessage(LOGTYPE.Error, "StackTrace: " + ex.StackTrace);
                DisposeTcpInstanceNClosingTcpConnection(fixEngineId);
            }
        }

        private static void DisposeTcpInstanceNClosingTcpConnection(string fixEngineId)
        {
            if(fixEngineId != null)
            {
                establishedFixEngineSockets.TryRemove(fixEngineId, out TcpClient tcpClient);

                if (tcpClient != null)
                {
                    tcpClient.Close();
                    Logging.LogMessage(LOGTYPE.Info, $"Connection with New FixEngine has been closed");
                }
            }
        }
    }
}
