using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.DataModels;

namespace FIXMonitorBusinessLogicLayer
{
    class FixEngineSocket
    {
        // has multiple socket handler
        // store and instantiate socket handler so it begins socket in constructor call

        readonly ConcurrentDictionary<string, SocketHandler> FixEngineSockets = new ConcurrentDictionary<string, SocketHandler>();
        private static FixEngineSocket fixEngineSocket = new FixEngineSocket();

        private FixEngineSocket() { }

        public void AddFixEngineSocket(FIXEngine fixEngine) {
            try
            {
                FixEngineSockets.TryAdd(fixEngine.engineID, new SocketHandler(fixEngine.FIXEngineIpAddress, fixEngine.FIXEngineIpPort, fixEngine.engineName));
            }
            catch (Exception ex) {
                CoreLogging.Logging.LogMessage(CoreLogging.LOGTYPE.Error, $"cannot instantiate socket engineid: {fixEngine.engineID} fixhubip: {fixEngine.FIXEngineIpAddress} fixhubport: {fixEngine.FIXEngineIpPort} ex {ex.Message}");
            }
            
        }

        public void AddFixEngineSocket(List<FIXEngine> fixEngines) {
            foreach (FIXEngine fixEngine in fixEngines) {
                AddFixEngineSocket(fixEngine);
            }
        }

        public static FixEngineSocket GetSingletonInstance() {
            return fixEngineSocket;
        }

        public SocketHandler GetFixEngineSocket(FIXEngine fixEngine) {
            SocketHandler socketHandler;
            FixEngineSockets.TryGetValue(fixEngine.engineID, out socketHandler);
            return socketHandler;
        }
    }
}
