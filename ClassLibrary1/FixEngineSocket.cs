using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.DataModels;
using CoreLogging;

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
                Logging.LogMessage(LOGTYPE.Info, $"Adding socket instance of FixEngine {fixEngine.engineName} on {fixEngine.FIXEngineIpAddress}:{fixEngine.FIXEngineIpPort}");
            }
            catch (Exception ex) {
                Logging.LogMessage(CoreLogging.LOGTYPE.Error, $"cannot instantiate socket engineid: {fixEngine.engineID} fixhubip: {fixEngine.FIXEngineIpAddress} fixhubport: {fixEngine.FIXEngineIpPort} ex {ex.Message}");
            }
        }

        public void RemoveFixEngineSocket(FIXEngine fixEngine)
        {
            SocketHandler socketHandler; // When this object of SocketHandler will go out of this function scope, its destructor will be called.
            FixEngineSockets.TryRemove(fixEngine.engineID, out socketHandler);
            Logging.LogMessage(LOGTYPE.Info, $"Removing socket instance of FixEngine {fixEngine.engineName} on {fixEngine.FIXEngineIpAddress}:{fixEngine.FIXEngineIpPort}");
            socketHandler.Dispose();
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
