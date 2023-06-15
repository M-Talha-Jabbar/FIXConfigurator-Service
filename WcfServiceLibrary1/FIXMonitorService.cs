using CoreLogging;
using FIXMonitorBusinessLogicLayer;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
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

        public FIXMonitorService()
        {
            Logging.StartProcessing(false);
        }

        public static FIXMonitorService GetInstance()
        {
            return service;
        }

        public FIXMonitorDataCache DataCache
        {
            get { return FIXMonitorDataCache.GetInstance(); }
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

        public IEnumerable<string> GetFixMessageTypesFilter()
        {
            return this.DataCache.GetFixMessageTypesFilter();
        }

        public IEnumerable<FIXSessionsConnectivityStatus> GetFixSessionsConnectivityStatus()
        {
            return this.DataCache.GetFixSessionsConnectivityStatus();
        }

        public void InvokeSessionUpdates(string engineName)
        {
            this.DataCache.InvokeSessionUpdates(engineName);
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

        public void SendQueuedUpdates()
        {
            if (callback == null || ((IChannel)callback).State != CommunicationState.Opened)
            {
                Logging.LogMessage(LOGTYPE.Info, "Initializing Callback");
                callback = OperationContext.Current.GetCallbackChannel<IFIXMonitorServiceCallback>();

                Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixSessionUpdate>(callback));
                Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixMessageUpdate>(callback));
                Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<ConfiguredFixMessage>(callback));
                Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<FixSessionStatusUpdate>(callback));
                Task.Run(() => ConcreteQueueCollectionsManager.SendQueuedUpdates<JenkinsJobUpdate>(callback));
            }
        }

        public void Subscribe(string connectionId)
        {
            Observable orderObservable = new Observable();
            OrderObserver observer = new OrderObserver();
            orderObservable.Subscribe(observer, connectionId);

            SendQueuedUpdates();
        }

        public bool IsSubscribed(string connectionId)
        {
            bool isSubscribed = Observable.IsSubscribed(connectionId);

            if (isSubscribed)
            {
                SendQueuedUpdates();
                Logging.LogMessage(LOGTYPE.Info, "[Observer] Client is connected: " + connectionId);
            }
                
            else
                Logging.LogMessage(LOGTYPE.Error, "[Observer] Client is disconnected: " + connectionId);

            return isSubscribed;
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
                Logging.LogMessage(LOGTYPE.Info, "Realtime FixMessageUpdate sent to Client");
                return;
            }

            FixMessageUpdate fixMessageUpdateItem = new FixMessageUpdate(fixMessage, engineID, sessionID);
            Queue.Enqueue(fixMessageUpdateItem);
            Logging.LogMessage(LOGTYPE.Info, "Queued FixMessageUpdate");
        }

        public void SendFixMessageWithConfiguredFixTagValuePairToClient(FIXMessage fixMessage, string engineID, string sessionID)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<ConfiguredFixMessage>();

            if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
            {
                callback.SendFixMessageWithConfiguredFixTagValuePairToClient(fixMessage, engineID, sessionID);
                Logging.LogMessage(LOGTYPE.Info, "Realtime FixMessageUpdate (i.e. for ConfiguredFixTagValuePair) sent to Client");
                return;
            }

            ConfiguredFixMessage configuredFixMessageItem = new ConfiguredFixMessage(fixMessage, engineID, sessionID);
            Queue.Enqueue(configuredFixMessageItem);
            Logging.LogMessage(LOGTYPE.Info, "Queued FixMessageUpdate (i.e. for ConfiguredFixTagValuePair)");
        }

        public void SendFixSessionToClient(FIXSession fixSession, string engineID, string commandType)
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<FixSessionUpdate>();

            if (((IChannel)callback).State == CommunicationState.Opened && Queue.Count == 0)
            {
                callback.SendFixSessionToClient(fixSession.GetClone(), engineID, commandType);
                Logging.LogMessage(LOGTYPE.Info, "Realtime FixSessionUpdate sent to Client");
                return;
            }

            FixSessionUpdate fixSessionUpdateItem = new FixSessionUpdate(fixSession, engineID, commandType);
            Queue.Enqueue(fixSessionUpdateItem);
            Logging.LogMessage(LOGTYPE.Info, "Queued FixSessionUpdate");
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
                Logging.LogMessage(LOGTYPE.Info, "Realtime FixSessionStatusUpdate sent to Client");
                return;
            }

            FixSessionStatusUpdate fixSessionStatusUpdateItem = new FixSessionStatusUpdate(fixSessionStatusMessage);
            Queue.Enqueue(fixSessionStatusUpdateItem);
            Logging.LogMessage(LOGTYPE.Info, "Queued FixSessionStatusUpdate");
        }

        public void SendJenkinsJobUpdate(JenkinsJobStatus jenkinsJobStatus) 
        {
            var Queue = ConcreteQueueCollectionsManager.CreateOrGetConcreteQueueCollection<JenkinsJobUpdate>();

            if (((IChannel)callback).State == CommunicationState.Opened)
            {
                callback.SendJenkinsJobUpdate(jenkinsJobStatus);
                Logging.LogMessage(LOGTYPE.Info, "Jenkins Job Status sent to client");
                return;
            }

            JenkinsJobUpdate jenkinsJobUpdate = new JenkinsJobUpdate(jenkinsJobStatus);
            Queue.Enqueue(jenkinsJobUpdate);
            Logging.LogMessage(LOGTYPE.Info, "Queued JenkinsJobUpdate");
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

        public IEnumerable<string> GetSessionStatusMessage()
        {
            return this.DataCache.GetSessionStatusMessage();
        }
        public async Task<string> TriggerJenkins(string branchName, string environment, string engineID)
        {
            return await DataCache.TriggerJenkins(branchName, environment, engineID);
        }

        public async Task<string> StartFixEngine(string engineID)
        {
            return await DataCache.StartFixEngine(engineID);
        }

        public async Task<string> StopFixEngine(string engineID)
        {
            return await DataCache.StopFixEngine(engineID);
        }

        public async Task<IEnumerable<string>> GetJenkinsSlaveNodes() 
        {
            return await DataCache.GetJenkinsSlaveNodes();
        }

        public async Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            return await DataCache.AddJenkinsConfiguration(fixEngineJenkinsConfiguration);
            
        }

        public async Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            return await DataCache.UpdateJenkinsConfiguration(fixEngineJenkinsConfiguration);
        }

        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string engineID)
        {
            return await DataCache.GetJenkinsConfiguration(engineID);
        }

        public async Task<bool> DeleteJenkinsConfiguration(string engineID)
        {
            return await DataCache.DeleteJenkinsConfiguration(engineID);
        }
        public async Task<JenkinsJobStatus> GetJenkinsLatestJobStatus()
        {
            return await this.DataCache.GetJenkinsLatestJobStatus();
        }
    }
}
