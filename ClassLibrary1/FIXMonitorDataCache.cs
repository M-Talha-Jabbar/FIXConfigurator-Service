using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using CoreLogging;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using RedisCacheService;
using FIXMonitorBusinessLogicLayer.IComparers;
using FIXMonitorBusinessLogicLayer.TcpConnection;
using FIXMonitorBusinessLogicLayer.Services;
using DevExtreme.AspNet.Data.ResponseModel;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;

namespace FIXMonitorBusinessLogicLayer
{
    public class FIXMonitorDataCache
    {
        private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        //Use IList instead of list
        private IList<FIXConfiguration> fixConfiguration;
        readonly Observable observable;
        private IFixHandler fixHandler;
        private IEmailHandler emailHandler;
        private IJenkinsService _jenkinsService;

        private readonly bool IsRunWithSampleData = Convert.ToBoolean(ConfigurationManager.AppSettings["isRunWithSampleData"].ToString());
        private readonly int WaitBeforeConnecting = Convert.ToInt32(ConfigurationManager.AppSettings["waitBeforeConnecting"].ToString());
        private readonly int HeartbeatIntervalForWeb = Convert.ToInt32(ConfigurationManager.AppSettings["heartbeatIntervalForWeb"].ToString());

        public FIXMonitorDataCache()
        {
            Logging.StartProcessing(false);
            observable = new Observable();
            InitAllCacheObjects();
            LoadStartUpData();
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

        private void TriggerUIRefresh()
        {
            Logging.LogMessage("TriggerUIRefresh Starts");
            InitAllCacheObjects();
            Logging.LogMessage("TriggerUIRefresh Ends");
        }

        private void InitAllCacheObjects()
        {
            fixConfiguration = new List<FIXConfiguration>();
            fixHandler = new FixHandler();
            emailHandler = new EmailHandler();
            _jenkinsService = new JenkinsService();
        }

        #region DataLoading

        private void LoadStartUpData()
        {
        }

        #endregion

        #region Inserting Data

        public void SaveFIXConfiguration(FIXConfiguration fixConfiguration)
        {
            //Perform the insertion in the database
            this.fixConfiguration.Add(fixConfiguration);
            //observable.SendFixSessionUpdate(fixConfiguration, fixConfiguration.SenderID, "insert");
        }

        #endregion

        #region ReturningData

        public IEnumerable<FIXConfiguration> GetFIXConfigurations()
        {
            return fixConfiguration;
        }


        public FixSessionKeyedCollection GetFixSessions(string FixEngineID)
        {
            return fixHandler.GetFixSession(FixEngineID);
        }

        #endregion

        #region GenerateMethods
        private IEnumerable<T> ParseListofObjects<T>(string location)
        {
            using (StreamReader sr = new StreamReader(location))
            {
                string json = sr.ReadToEnd();
                return Newtonsoft.Json.JsonConvert.DeserializeObject<List<T>>(json);
            }
        }

        #endregion

        #region Fix
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
        #endregion

        #region Alerts

        public List<AlertFlag> GetAlertCache()
        {
            return new List<AlertFlag>();
        }
        public bool RemoveAlertCache(string orderId)
        {
            return true;
        }

        #endregion

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

        public async Task<string> TriggerJenkins(string branchName, string environment, string FixEngineIpAndPort)
        {
            return await _jenkinsService.JenkinsTrigger(branchName, environment, FixEngineIpAndPort);
        }

        public async Task<string> StartFixEngine(string engineID)
        {
            return await _jenkinsService.StartFixEngine(engineID);
        }

        public async Task<string> StopFixEngine(string FixEngineIpAndPort)
        {
            return await _jenkinsService.StopFixEngine(FixEngineIpAndPort);
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
        public async Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string FixEngineIpAndPort)
        {
            return await _jenkinsService.GetJenkinsConfiguration(FixEngineIpAndPort);
        }
        public async Task<bool> DeleteJenkinsConfiguration(string FixEngineIpAndPort)
        {
            return await _jenkinsService.DeleteJenkinsConfiguration(FixEngineIpAndPort);
        }

        public async Task<JenkinsJobStatus> GetJenkinsLatestJobStatus()
        {
            return await _jenkinsService.GetJenkinsLatestJobStatus();
        }
    }
}