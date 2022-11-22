using FIXMonitorBusinessLogicLayer;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using System;
using System.Collections.Generic;
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Channels;

namespace FIXMonitorService
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the class name "FIXMonitorService" in both code and config file together.
    [ServiceBehavior(ConcurrencyMode = ConcurrencyMode.Multiple, InstanceContextMode = InstanceContextMode.Single)]
    public class FIXMonitorService : IFIXMonitorService
    {
        static IFIXMonitorServiceCallback callback;

        readonly private static FIXMonitorService service = new FIXMonitorService();

        private FIXMonitorService()
        {
        }

        public static FIXMonitorService GetInstance()
        {
            return service;
        }

        public FIXMonitorDataCache DataCache
        {
            get { return FIXMonitorDataCacheWrapper.GetInstance().GetATSDataCache(); }
        }

        public string GetData(int value)
        {
            return string.Format("You entered: {0}", value);
        }

        public CompositeType GetDataUsingDataContract(CompositeType composite)
        {
            if (composite == null)
            {
                throw new ArgumentNullException("composite");
            }
            if (composite.BoolValue)
            {
                composite.StringValue += "Suffix";
            }
            return composite;
        }

        public void AddFIXConfiguration(FIXConfiguration fixConfiguration)
        {
            this.DataCache.SaveFIXConfiguration(fixConfiguration);
        }

        public IEnumerable<FIXConfiguration> GetFIXConfigurations()
        {
            return this.DataCache.GetFIXConfigurations();
        }

        public FixSessionKeyedCollection GetFixSessions(string FixEngineID)
        {
            return this.DataCache.GetFixSessions(FixEngineID);
        }

        public bool ConnectToFIX(FIXSession fixSession)
        {
            return this.DataCache.ConnectFixSession(fixSession);
        }

        public bool DisconnectToFIX(FIXSession fixSession)
        {
            return this.DataCache.DisconnectFixSession(fixSession);
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            return this.DataCache.ResetSequenceNumber(fixSession);
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            return this.DataCache.SetSequenceNumber(fixSession);
        }

        public FixEnginesKeyedCollection GetFixEngines()
        {
            return this.DataCache.GetFixEngines();
        }

        public FIXEngine ConnectToFixEngine(FIXEngine fixEngine)
        {
            return this.DataCache.ConnectToFixEngine(fixEngine);
        }

        public FIXEngine DisconnectToFixEngine(FIXEngine fixEngine)
        {
            return DataCache.DisconnectToFixEngine(fixEngine);
        }

        public FIXSession ConnectToFixSession(string engineID, FIXSession fixSession)
        {
            return this.DataCache.ConnectToFixSession(engineID, fixSession);
        }

        public void Subscribe(string connectionId)
        {
            callback = OperationContext.Current.GetCallbackChannel<IFIXMonitorServiceCallback>();
            Observable orderObservable = new Observable();
            OrderObserver observer = new OrderObserver();
            orderObservable.Subscribe(observer, connectionId);
        }

        public bool IsSubscribed(string connectionId)
        {
            Observable orderObservable = new Observable();
            return orderObservable.IsSubscribed(connectionId);
        }

        public IEnumerable<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            return this.DataCache.GetFixMessages(fixEngineID, fixSessionConnectionID, dataSourceLoadOptions);
        }

        public void SendFixMessagesToClient(FIXMessage fixMessage, string engineID, string sessionID)
        {
            if (((IChannel)callback).State == CommunicationState.Opened)
            {
                callback.SendFixMessagesToClient(fixMessage, engineID, sessionID);
            }
        }

        public void SendFixSessionToClient(FIXSession fixSession, string engineID, string commandType)
        {
            if (((IChannel)callback).State == CommunicationState.Opened)
            {
                callback.SendFixSessionToClient(fixSession, engineID, commandType);
            }
        }

        public void Heartbeat()
        {
            if (((IChannel)callback).State == CommunicationState.Opened)
            {
                callback.Heartbeat();
            }
        }

        public void SendAlertFlag(AlertFlag flag)
        {
            if (((IChannel)callback).State == CommunicationState.Opened)
            {
                callback.SendAlertFlag(flag);
            }
        }

        public List<AlertFlag> GetAlertCache()
        {
            return this.DataCache.GetAlertCache();
        }
        public bool RemoveAlertCache(string orderId)
        {
            return this.DataCache.RemoveAlertCache(orderId);
        }

        public SessionEmails GetSessionAlertConfiguration(string SessionId)
        {
            return this.DataCache.GetSessionAlertConfiguration(SessionId);
        }

        public bool AddSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            return this.DataCache.AddSessionAlertConfiguration(sessionEmails);
        }

        public bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            return this.DataCache.UpdateSessionAlertConfiguration(sessionEmails);
        }

        public bool DeleteSessionAlertConfiguration(string SessionId)
        {
            return this.DataCache.DeleteSessionAlertConfiguration(SessionId);
        }

        public List<FIXMessageRejects> GetAllFixMessageRejects()
        {
            return this.DataCache.GetAllFixMessageRejects();
        }

        public bool AddFixMessageReject(FIXMessageRejects fixMessageRejects)
        {
            return this.DataCache.AddFixMessageReject(fixMessageRejects);
        }

        public bool DeleteFixMessageReject(string FixTag, string FixValue)
        {
            return this.DataCache.DeleteFixMessageReject(FixTag, FixValue);
        }

        public Stream GetFixMessageLogFileStream(string sessionId, string engineName)
        {
            return this.DataCache.GetFixMessageLogFileStream(sessionId, engineName);
        }

        public bool FileExists(string sessionId, string engineName) 
        {
            return this.DataCache.FileExists(sessionId, engineName);
        }

        public FIXEngine GetFixEngine(string engineID)
        {
            return this.DataCache.GetFixEngine(engineID);
        }
    }
}
