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
    public static class SocketHandler
    {
        static string ipAddress = System.Configuration.ConfigurationManager.AppSettings["fixHubServerIP"].ToString();
        static int port = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["fixHubServerPort"].ToString());
        static int heartbeat = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["heartbeatIntervalForFixHub"].ToString());
        static int waitBeforeConnecting = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["waitBeforeConnecting"].ToString());
        static Socket _socket;
        private static bool isConnected = false;
        static IPEndPoint IPEndPoint;
        static Thread t;

        public static BehaviorSubject<bool> subject;

        static SocketHandler()
        {
            IPEndPoint = new IPEndPoint(IPAddress.Parse(ipAddress), port);
            subject = new BehaviorSubject<bool>(true);
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            Logging.LogMessage(LOGTYPE.Info, "Begin Socket Connection.");
            _socket.BeginConnect(IPEndPoint, SocketConnectCallback, _socket);
        }

        private static void SocketConnectCallback(IAsyncResult ar)
        {
            Logging.LogMessage(LOGTYPE.Info, "Inside Connection Callback.");
            try
            {
                var _socket = (Socket)ar.AsyncState;
                _socket.EndConnect(ar);
                if (_socket.Connected)
                {
                    SocketHandler._socket = _socket;
                    Logging.LogMessage(LOGTYPE.Info, "Socket Successfully Connected.");
                    isConnected = true;
                    subject.OnNext(true);
                    Logging.LogMessage(LOGTYPE.Debug, "Socket is Alive");
                    t = new Thread(() => { IsConnected(_socket); });
                    t.Start();
                }
            }
            catch(Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Socket Connection Failed \n " + e.Message);
                Reconnect();
            }
        }

        private static void Reconnect()
        {
            try
            {
                Thread.Sleep(waitBeforeConnecting);
                if (_socket.Connected)
                {
                    _socket.Disconnect(true);
                    isConnected = false;
                    subject.OnNext(false);
                }
                _socket.BeginConnect(IPEndPoint, SocketConnectCallback, _socket);
            }
            catch(Exception e)
            {
                Logging.LogMessage(LOGTYPE.Error, "Socket Connection Failed \n " + e.Message);
                Reconnect();
            }
        }

        private static void IsConnected(this Socket s)
        {
            while (isConnected)
            {
                var pollResult = !((s.Poll(1000, SelectMode.SelectRead) && (s.Available == 0)));
                if (pollResult)
                {
                    //Logging.LogMessage(LOGTYPE.Debug, "Socket is Alive");
                    isConnected = true;
                    subject.OnNext(true);
                }
                else
                {
                    isConnected = false;
                    subject.OnNext(false);
                    Logging.LogMessage(LOGTYPE.Error, "Socket Disconnected");
                    Reconnect();
                }
            }
        }

        public static IObservable<bool> GetStatus()
        {
            return subject;
        }
    }
}
