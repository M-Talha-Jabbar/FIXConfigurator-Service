using CoreLogging;
using FIXMonitorBusinessLogicLayer.Utilities;
using System;
using System.Collections.Concurrent;
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
        private BehaviorSubject<bool> subject = null;
        public bool isConnected;

        public SocketListener(bool isConnected)
        {
            this.isConnected = isConnected;
            this.subject = new BehaviorSubject<bool>(isConnected);
        }

        public static async Task ListenClientsAsync(bool listening) // Listening FixEngine Clients
        {
            TcpListener listener = new TcpListener(IPAddress.Any, socketListeningPort);
            listener.Start();
            Logging.LogMessage(LOGTYPE.Info, $"Start Listening to FixEngine Clients");

            while (listening)
            {
                TcpClient tcpClient = await listener.AcceptTcpClientAsync();
                Logging.LogMessage(LOGTYPE.Info, $"New FixEngine connected");
                
                HandleClientAsync(tcpClient); // If you don’t want your method execution to wait for the asynchronous method to complete its execution, then, in that case, you need to use the return type of the asynchronous method to void.
            }
        }

        private static async void HandleClientAsync(TcpClient tcpClient) 
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
                    
                    if (bytesRead == 0) // When 0 bytes is read, it means that the opposite party has disconnected the socket.
                    {
                        socketListener.isConnected = false;
                        socketListener.subject.OnNext(socketListener.isConnected);

                        // We will not remove socket instance from fixEngineSocketConnections collection in FixConfigurator on disconnection with a FixEngine socket.

                        DisposeTcpInstanceNClosingTcpConnection(tcpClient);

                        break;
                    }

                    if (!isEngineIdReceived)
                    {
                        string fixEngineId = Encoding.ASCII.GetString(buffer, 0, bytesRead);

                        bool isInstanceCreated = fixEngineSocketConnections.TryGetValue(fixEngineId, out SocketListener value);

                        if (isInstanceCreated) // If Engine has already been created in FixConfigurator
                        {
                            socketListener = value;
                            socketListener.isConnected = true;
                            socketListener.subject.OnNext(socketListener.isConnected);
                        }

                        else // If Engine has not yet created in FixConfigurator
                            socketListener = new SocketListener(isConnected: true);

                        socketListener.fixEngineId = fixEngineId;
                        socketListener.tcpClient = tcpClient;

                        fixEngineSocketConnections.TryAdd(fixEngineId, socketListener);
                        isEngineIdReceived = true;
                    }
                }
            }
            catch(Exception e) // Exception is thrown when socket connection is forcbily closed. 
            {
                ExceptionLoggingUtility.LogException(e, null);

                if (socketListener != null)
                {
                    socketListener.isConnected = false;
                    socketListener.subject.OnNext(socketListener.isConnected);
                    fixEngineSocketConnections.TryRemove(socketListener.fixEngineId, out SocketListener value);
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
