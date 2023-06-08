using CoreLogging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.TcpConnection
{
    class TcpConnection
    {
        public string _ipAddress;
        public int _portNo;

        public TcpConnection(string ipAddress, int portNo) {

            _ipAddress = ipAddress;
            _portNo = portNo;
        }

        public bool TcpConnectionBuilder()
        {
            TcpClient client = new TcpClient();

            try
            {
                client.Connect(_ipAddress, _portNo);
                Logging.LogMessage(LOGTYPE.Info, $"Tcp connection established with: {_ipAddress}:{_portNo}");
                return true;
            }
            catch (Exception ex)
            {
                Logging.LogMessage(LOGTYPE.Error, $"Cannot Estalish Tcp connection with: {_ipAddress}:{_portNo} , error : {ex.Message}");
                return false;
            }
            finally
            {
                if (client.Connected)
                {
                    client.Close();
                }
            }
        }

    }
}
