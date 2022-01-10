using CoreLogging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public static class SocketHandler
    {
        static string ipAddress = "127.0.0.1";
        static int port = 7890;
        static Socket _socket;
        static SocketHandler()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            Logging.LogMessage(LOGTYPE.Info, "Begin Socket Connection.");
            _socket.BeginConnect(new IPEndPoint(IPAddress.Parse(ipAddress), port), SocketConnectCallback, _socket);
        }
        private static void SocketConnectCallback(IAsyncResult ar)
        {
            Logging.LogMessage(LOGTYPE.Info, "Inside Connection Callback.");
            var _socket = (Socket)ar.AsyncState;
            _socket.EndConnect(ar);
            if (_socket.Connected)
            {
                SocketHandler._socket = _socket;
                Logging.LogMessage(LOGTYPE.Info, "Socket Successfully Connected.");
                IsConnected(_socket);
            }
        }
        private static bool IsConnected(this Socket socket)
        {
            try
            {
                return !(socket.Poll(1, SelectMode.SelectRead) && socket.Available == 0);
            }
            catch (SocketException)
            {
                Logging.LogMessage(LOGTYPE.Info, "Socket Connection Lost.");
                return false;
            }
        }
        public static void SetIpAndPort(string ip, int port)
        {
            ipAddress = ip;
            SocketHandler.port = port;
        }
    }
}
