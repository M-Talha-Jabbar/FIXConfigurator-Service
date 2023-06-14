using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using CoreLogging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Threading.Tasks;
using FIXMonitorBusinessLogicLayer.Services;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;

namespace FIXMonitorBusinessLogicLayer
{
    public sealed class FIXMonitorDataCache
    {
        private static FIXMonitorDataCache _FIXMonitorDataCache = null;
        private static readonly object Instancelock = new object();
        private IList<FIXConfiguration> fixConfiguration;
        private readonly Observable observable;
        private IFixHandler fixHandler;
        private IEmailHandler emailHandler;
        private IJenkinsService _jenkinsService;
        private readonly int HeartbeatIntervalForWeb = Convert.ToInt32(ConfigurationManager.AppSettings["heartbeatIntervalForWeb"].ToString());

        public static FIXMonitorDataCache GetInstance()
        {
            if(_FIXMonitorDataCache == null)
            {
                lock (Instancelock)
                {
                    if (_FIXMonitorDataCache == null)
                        _FIXMonitorDataCache = new FIXMonitorDataCache();
                }
            }

            return _FIXMonitorDataCache;
        }

        private FIXMonitorDataCache()
        {
            observable = new Observable();
            InitAllCacheObjects();
            Task.Run(() => HeartbeatToWeb());
        }

        public async Task HeartbeatToWeb()
        {
            while (true)
            {
                observable.Heartbeat();
                await Task.Delay(HeartbeatIntervalForWeb);
            }
        }

        private void InitAllCacheObjects()
        {
            fixConfiguration = new List<FIXConfiguration>();
            fixHandler = new FixHandler();
            emailHandler = new EmailHandler();
            _jenkinsService = new JenkinsService();
        }

        public void SaveFIXConfiguration(FIXConfiguration fixConfiguration)
        {
            this.fixConfiguration.Add(fixConfiguration);
        }

        public IEnumerable<FIXConfiguration> GetFIXConfigurations()
        {
            return fixConfiguration;
        }

        public FixSessionKeyedCollection GetFixSessions(string FixEngineID)
        {
            return fixHandler.GetFixSession(FixEngineID);
        }
        private IEnumerable<T> ParseListofObjects<T>(string location)
        {
            using (StreamReader sr = new StreamReader(location))
            {
                string json = sr.ReadToEnd();
                return Newtonsoft.Json.JsonConvert.DeserializeObject<List<T>>(json);
            }
        }

        public bool ConnectFixSession(FIXSession fixSession)
        {
            return fixHandler.ConnectFixSessionAsync(fixSession);
        }

        public bool DisconnectFixSession(FIXSession fixSession)
        {
            return fixHandler.DisconnectFixSession(fixSession);
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            return fixHandler.ResetSequenceNumber(fixSession);
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            return fixHandler.SetSequenceNumber(fixSession);
        }

        public FixEnginesKeyedCollection GetFixEngines()
        {
            return fixHandler.GetFixEngines();
        }

        public IEnumerable<FIXSessionsConnectivityStatus> GetFixSessionsConnectivityStatus()
        {
            return fixHandler.GetFixSessionsConnectivityStatus();
        }

        public void InvokeSessionUpdates(string engineName)
        {
            fixHandler.InvokeSessionUpdates(engineName);
        }

        public FIXEngine ConnectToFixEngine(FIXEngine fixEngine)
        {
            return fixHandler.ConnectToFixEngine(fixEngine);
        }

        public FIXEngine DisconnectToFixEngine(FIXEngine fixEngine)
        {
            return fixHandler.DisconnectToFixEngine(fixEngine);
        }

        public FIXSession ConnectToFixSession(string engineID, FIXSession fixSession)
        {
            return fixHandler.ConnectToFixSession(engineID, fixSession);
        }

        public string GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            return fixHandler.GetFixMessagesAsync(fixEngineID, fixSessionConnectionID, dataSourceLoadOptions);
        }

        public List<FIXMessage> GetFixMessagesHavingAnyConfiguredFixTagValuePair(string sessionID)
        {
            return fixHandler.GetFixMessagesHavingAnyConfiguredFixTagValuePair(sessionID);
        }

        public List<AlertFlag> GetAlertCache()
        {
            return new List<AlertFlag>();
        }
        public bool RemoveAlertCache(string orderId)
        {
            return true;
        }

        public SessionEmails GetSessionAlertConfiguration(string SessionId)
        {
            return emailHandler.GetSessionAlertConfiguration(SessionId);
        }
        public bool AddSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            return emailHandler.AddSessionAlertConfiguration(sessionEmails);
        }

        public bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails)
        {
            return emailHandler.UpdateSessionAlertConfiguration(sessionEmails);
        }

        public bool DeleteSessionAlertConfiguration(string SessionId)
        {
            return emailHandler.DeleteSessionAlertConfiguration(SessionId);
        }

        public List<FixTagValueConfiguration> GetAllFixMessageConfiguration()
        {
            return emailHandler.GetAllFixMessageConfiguration();
        }

        public bool AddFixMessageConfiguration(FixTagValueConfiguration fixTagValueConfiguration)
        {
            return emailHandler.AddFixMessageConfiguration(fixTagValueConfiguration);
        }

        public bool DeleteFixMessageConfiguration(int id)
        {
            //fixHandler.FiltrationOfFixMessagesWithRespectToCurrentConfiguredTagValuePairs(id);
            return emailHandler.DeleteFixMessageConfiguration(id);
        }

        public bool FileExists(string sessionId, string engineName)
        {
            return FixMessageLog.GetFixMessageLogFilePath(sessionId, engineName) == null ? false : true;
        }

        public Stream GetFixMessageLogFileStream(string sessionId, string engineName)
        {
            string filepath = FixMessageLog.GetFixMessageLogFilePath(sessionId, engineName);
            return FileStreamExport.fsExport(filepath, FixMessageLog.locksforConcurrentFileAccess.GetLockObj(filepath));
        }

        public FIXEngine GetFixEngine(string engineID)
        {
            return fixHandler.GetFixEngine(engineID);
        }

        public bool TcpConnection(string ipAddress, int port)
        {
            return new TcpConnection.TcpConnection(ipAddress, port).TcpConnectionBuilder();
        }

        public IEnumerable<string> GetSessionStatusMessage()
        {
            return fixHandler.GetSessionStatusMessage();
        }

        public async Task<string> TriggerJenkins(string branchName, string environment)
        {
            return await _jenkinsService.JenkinsTrigger(branchName, environment);
        }

        public async Task<string> TriggerJenkins(string branchName, string environment, string engineID)
        {
            return await _jenkinsService.JenkinsTrigger(branchName, environment, engineID);
        }

        public async Task<string> StartFixEngine(string engineID)
        {
            return await _jenkinsService.StartFixEngine(engineID);
        }

        public async Task<string> StopFixEngine(string engineID)
        {
            return await _jenkinsService.StopFixEngine(engineID);
        }

        public async Task<IEnumerable<string>> GetJenkinsSlaveNodes()
        {
            return await _jenkinsService.GetJenkinsSlaveNodes();
        }

        public async Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            return await _jenkinsService.AddJenkinsConfiguration(fixEngineJenkinsConfiguration);
        }
        public async Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration)
        {
            return await _jenkinsService.UpdateJenkinsConfiguration(fixEngineJenkinsConfiguration);
        }
        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string engineID)
        {
            return await _jenkinsService.GetJenkinsConfiguration(engineID);
        }
        public async Task<bool> DeleteJenkinsConfiguration(string engineID)
        {
            return await _jenkinsService.DeleteJenkinsConfiguration(engineID);
        }

        public async Task<JenkinsJobStatus> GetJenkinsLatestJobStatus()
        {
            return await _jenkinsService.GetJenkinsLatestJobStatus();
        }
    }
}