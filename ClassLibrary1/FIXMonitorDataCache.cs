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
        
        //Can use inherited class instead of creating object.

        private readonly bool IsRunWithSampleData = Convert.ToBoolean(ConfigurationManager.AppSettings["isRunWithSampleData"].ToString());
        private readonly int WaitBeforeConnecting = Convert.ToInt32(ConfigurationManager.AppSettings["waitBeforeConnecting"].ToString());
        private readonly int HeartbeatIntervalForWeb = Convert.ToInt32(ConfigurationManager.AppSettings["heartbeatIntervalForWeb"].ToString());
        private readonly int timeoutInterval = 5000;

        public FIXMonitorDataCache()
        {
            Logging.StartProcessing(false);
            observable = new Observable();
            InitAllCacheObjects();
            LoadStartUpData();
            Thread HeatbeatSendingThread = new Thread(new ThreadStart(HeatbeatToWeb));
            HeatbeatSendingThread.Start();
        }

        public void HeatbeatToWeb()
        {
            while (true)
            {
                Task.Run(() => observable.Heartbeat());
                Thread.Sleep(HeartbeatIntervalForWeb);
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

        public IEnumerable<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            return fixHandler.GetFixMessages(fixEngineID, fixSessionConnectionID, dataSourceLoadOptions);
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

        public List<FIXMessageRejects> GetAllFixMessageRejects()
        {
            return emailHandler.GetAllFixMessageRejects();
        }

        public bool AddFixMessageReject(FIXMessageRejects fixMessageRejects)
        {
            return emailHandler.AddFixMessageReject(fixMessageRejects);
        }

        public bool DeleteFixMessageReject(int id)
        {
            return emailHandler.DeleteFixMessageReject(id);
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

        public FIXEngine GetFixEngine(string engineID) { 
            return fixHandler.GetFixEngine(engineID);
        }
    }

}
