using CoreLogging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class SocketListener
    {
        private static int socketListeningPort = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["socketListeningPort"].ToString());
        public static ConcurrentDictionary<string, SocketListener> fixEngineSocketConnections = new ConcurrentDictionary<string, SocketListener>();
        private string fixEngineId;
        private TcpClient tcpClient;
        private BehaviorSubject<bool> subject;
        private bool isConnected;

        public SocketListener()
        {
            this.isConnected = false;
            this.subject = new BehaviorSubject<bool>(isConnected);
        }

        public static async Task ListenClientsAsync() // Listening FixEngine Clients
        {
            TcpListener listener = new TcpListener(IPAddress.Any, socketListeningPort);
            listener.Start();
            Logging.LogMessage(LOGTYPE.Info, $"Start Listening to FixEngine Clients");

            while (true)
            {
                TcpClient tcpClient = await listener.AcceptTcpClientAsync();
                Logging.LogMessage(LOGTYPE.Info, $"New FixEngine connected");
                
                HandleClientAsync(tcpClient);
            }
        }

        private static async Task HandleClientAsync(TcpClient tcpClient) 
        {
            SocketListener socketListener = null;
            bool isEngineIdReceived = false;

            try
            {
                NetworkStream stream = tcpClient.GetStream();

                while (true)
                {
                    byte[] buffer = new byte[256];
                    int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);

                    if (!isEngineIdReceived)
                    {
                        string fixEngineId = Encoding.ASCII.GetString(buffer, 0, bytesRead);

                        // proto modal of fixengine

                        bool isInstanceCreated = fixEngineSocketConnections.TryGetValue(fixEngineId, out SocketListener value);
                        if (isInstanceCreated) // If Engine has already been created in FixConfigurator
                            socketListener = value;
                        else // If Engine has not yet created in FixConfigurator
                            socketListener = new SocketListener();

                        socketListener.fixEngineId = fixEngineId;
                        socketListener.tcpClient = tcpClient;
                        socketListener.isConnected = true;
                        socketListener.subject.OnNext(socketListener.isConnected);
                        fixEngineSocketConnections.TryAdd(fixEngineId, socketListener);

                        isEngineIdReceived = true;
                    }
                }
            }
            catch(Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, "Exception: " + ex.Message);
                Logging.LogMessage(LOGTYPE.Error, "StackTrace: " + ex.StackTrace);

                if(socketListener != null)
                {
                    socketListener.isConnected = false;
                    socketListener.subject.OnNext(socketListener.isConnected);
                    fixEngineSocketConnections.TryRemove(socketListener.fixEngineId, out SocketListener socketListenerInstance);
                }

                DisposeTcpInstanceNClosingTcpConnection(tcpClient);
            }
        }

        private static void DisposeTcpInstanceNClosingTcpConnection(TcpClient tcpClient)
        {
            if (tcpClient != null)
            {
                tcpClient.Close();
                Logging.LogMessage(LOGTYPE.Info, $"Connection with New FixEngine has been closed");
            }
        }

        public IObservable<bool> GetStatus()
        {
            return subject;
        }
    }
}
