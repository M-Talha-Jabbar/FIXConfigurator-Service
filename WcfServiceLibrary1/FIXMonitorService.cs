using DevExtreme.AspNet.Data.ResponseModel;
using FIXMonitorBusinessLogicLayer;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using FIXMonitorService.PayLoads;
using FIXMonitorService.QueueManager;
using System;
using System.Collections.Generic;
using System.IO;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Threading.Tasks;

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

        public IEnumerable<FIXSessionsConnectivityStatus> GetFixSessionsConnectivityStatus()
        {
            return this.DataCache.GetFixSessionsConnectivityStatus();
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

            Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixSessionUpdate>(callback));
            Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixMessageUpdate>(callback));
            Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<ConfiguredFixMessage>(callback));
            Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixSessionStatusUpdate>(callback));

            Observable orderObservable = new Observable();
            OrderObserver observer = new OrderObserver();
            orderObservable.Subscribe(observer, connectionId);
        }

        public bool IsSubscribed(string connectionId)
        {
            Observable orderObservable = new Observable();
            return orderObservable.IsSubscribed(connectionId);
        }

        public string GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            return this.DataCache.GetFixMessages(fixEngineID, fixSessionConnectionID, dataSourceLoadOptions);
        }

        public List<FIXMessage> GetFixMessagesHavingAnyConfiguredFixTagValuePair(string sessionID)
        {
            return this.DataCache.GetFixMessagesHavingAnyConfiguredFixTagValuePair(sessionID);
        }

        public void SendFixMessagesToClient(FIXMessage fixMessage, string engineID, string sessionID)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<FixMessageUpdate>();

            if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
            {
                callback.SendFixMessagesToClient(fixMessage, engineID, sessionID);
                Console.WriteLine("Realtime FixMessageUpdate sent to Client");
                return;
            }

            FixMessageUpdate fixMessageUpdateItem = new FixMessageUpdate(fixMessage, engineID, sessionID);
            Queue.Enqueue(fixMessageUpdateItem);
            Console.WriteLine("Queued FixMessageUpdate");
        }

        public void SendFixMessageWithConfiguredFixTagValuePairToClient(FIXMessage fixMessage, string engineID, string sessionID)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<ConfiguredFixMessage>();

            if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
            {
                callback.SendFixMessageWithConfiguredFixTagValuePairToClient(fixMessage, engineID, sessionID);
                Console.WriteLine("Realtime FixMessageUpdate (i.e. for ConfiguredFixTagValuePair) sent to Client");
                return;
            }

            ConfiguredFixMessage configuredFixMessageItem = new ConfiguredFixMessage(fixMessage, engineID, sessionID);
            Queue.Enqueue(configuredFixMessageItem);
            Console.WriteLine("Queued FixMessageUpdate (i.e. for ConfiguredFixTagValuePair)");
        }

        public void SendFixSessionToClient(FIXSession fixSession, string engineID, string commandType)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<FixSessionUpdate>();
            try
            {
                if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
                {
                    callback.SendFixSessionToClient(fixSession.GetClone(), engineID, commandType);
                    Console.WriteLine("Realtime FixSessionUpdate sent to Client");
                    return;
                }
            
                FixSessionUpdate fixSessionUpdateItem = new FixSessionUpdate(fixSession, engineID, commandType);
                Queue.Enqueue(fixSessionUpdateItem);
                Console.WriteLine("Queued FixSessionUpdate");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString(), ex.Message);
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

        public void SendFixSessionStatusMessage(string fixSessionStatusMessage)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<FixSessionStatusUpdate>();

            if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
            {
                callback.SendFixSessionStatusMessage(fixSessionStatusMessage);
                Console.WriteLine("Realtime FixSessionStatusUpdate sent to Client");
                return;
            }

            FixSessionStatusUpdate fixSessionStatusUpdateItem = new FixSessionStatusUpdate(fixSessionStatusMessage);
            Queue.Enqueue(fixSessionStatusUpdateItem);
            Console.WriteLine("Queued FixSessionStatusUpdate");
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

        public List<FixTagValueConfiguration> GetAllFixMessageConfiguration()
        {
            return this.DataCache.GetAllFixMessageConfiguration();
        }

        public bool AddFixMessageConfiguration(FixTagValueConfiguration fixTagValueConfiguration)
        {
            return this.DataCache.AddFixMessageConfiguration(fixTagValueConfiguration);
        }

        public bool DeleteFixMessageConfiguration(int id)
        {
            return this.DataCache.DeleteFixMessageConfiguration(id);
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

        public bool TcpConnection(string ipAddress, int port)
        {
            return this.DataCache.TcpConnection(ipAddress, port);
        }

        public IEnumerable<string> GetSessionStatusMessage() {
            return this.DataCache.GetSessionStatusMessage();
        }
    }
}
