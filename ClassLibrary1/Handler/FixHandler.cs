using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.IHandler;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
//using FIXMonitorServer;
using RedisCacheService;
using StackExchange.Redis;
using CoreLogging;
using proto;
using System.Reactive.Subjects;
using FIXMonitorBusinessLogicLayer.Momentos;
using FBE.proto;
using DevExtreme.AspNet.Mvc;
using Newtonsoft.Json;
using DevExtreme.AspNet.Data;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.Notifier;
using System.Collections.Concurrent;
using FIXMonitorBusinessLogicLayer.Converter;
using FIXMonitorBusinessLogicLayer.LocksManager;
using DevExtreme.AspNet.Data.ResponseModel;
using FIXMonitorBusinessLogicLayer.Utilities;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class FixHandler : IFixHandler
    {
        private FixEnginesKeyedCollection fixEngines;
        private Dictionary<string, int> fixEnginesDB;
        private Dictionary<string, Channel> fixEnginesChannels;
        public static Dictionary<string, string> fixMsgTypes = new Dictionary<string, string>();
        public static Dictionary<string, string> fixTagValues = new Dictionary<string, string>();
        Observable observable = new Observable();
        private readonly bool sendSampleFixUpdate = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings["sendSampleFixUpdate"].ToString());
        private readonly string redisStreamName = System.Configuration.ConfigurationManager.AppSettings["redisStreamName"].ToString();
        private readonly string fixEngineRedisConfigFilePath = "redisConfigAndDB.txt";

        //Messages Stream Attributes
        private Dictionary<string, string> streamLastReadTimeStamps;

        //Status Stream Attributes
        private List<RedisValue> statusReadMessagesIDs;

        private Dictionary<string, List<FIXMessage>> sessionFixMessages;
        private Dictionary<string, List<FIXMessage>> fixMessagesContainingConfiguredFixTagValuePair;
        private Dictionary<string, bool> hasSessionsBeenCreatedForAEngine; // Created for a purpose if update comes first before reading existing messages as since we are calling SubscribeAndFaliureCallback() before ReadMessages() while creating a FixEngine at the middle of the day.

        private ConcurrentDictionary<string, FixEngineMomento> fixEngineMomentos;

        private EmailNotifier emailNotifier;

        private FixEngineSocket fixEngineSocket;

        private ConcurrentStack<string> sessionStatuses;

        private LockObjectsManager locksForHandlingStreamRead;
        private LockObjectsManager sessionUpdatesLocks;

        private const bool existingMessage = false;
        private const bool realTimeMessage = true;

        public FixHandler()
        {
            Initializers();
            EnginePersistence();

            if (sendSampleFixUpdate)
            {
                Task.Run(async () => await SendSampleFixMessages());
            }

            LoadFIXEnginesAndSessions();

            //Save The updated configuration to the file 
            PersistFixEngineConfig();

            Task.Run(() => { SetScheduler(); });
        }

        private void EnginePersistence()
        {
            IEnumerable<string> FixEnginesRedisConfig = FetchFixEngineRedisConfig();

            foreach (var row in FixEnginesRedisConfig)
            {
                var columns = LineSplitterWithAString(row);
                InsertFixEngineRedisConfigInFixEnginesDB(row, Int32.Parse(columns[1]));
            }
        }

        public IEnumerable<string> FetchFixEngineRedisConfig()
        {
            IEnumerable<string> data = new List<string>();

            if (File.Exists(fixEngineRedisConfigFilePath))
            {
                data = File.ReadAllLines(fixEngineRedisConfigFilePath).ToList();

                return data;
            }

            return data;
        }

        public string[] LineSplitter(string line, char splitter = ':') 
        { 
            return line.Split(splitter); // Default string splitter set to ":"
        }

        public string[] LineSplitterWithAString(string line, string[] splitter = null)
        {
            return line.Split(splitter ?? new string[] { "::" }, System.StringSplitOptions.RemoveEmptyEntries); // Default string splitter set to "::"
        }

        public void InsertFixEngineRedisConfigInFixEnginesDB(string engineID, int db)
        {
            if (!fixEnginesDB.ContainsKey(engineID))
            {
                fixEnginesDB.Add(engineID, db);
            }
        }

        private void Initializers()
        {
            fixEngines = new FixEnginesKeyedCollection(); 
            fixEnginesDB = new Dictionary<string, int>();
            fixEnginesChannels = new Dictionary<string, Channel>();
            statusReadMessagesIDs = new List<RedisValue>();
            streamLastReadTimeStamps = new Dictionary<string, string>();
            sessionFixMessages = new Dictionary<string, List<FIXMessage>>();
            fixMessagesContainingConfiguredFixTagValuePair = new Dictionary<string, List<FIXMessage>>();
            hasSessionsBeenCreatedForAEngine = new Dictionary<string, bool>();
            fixEngineSocket = FixEngineSocket.GetSingletonInstance();
            fixEngineMomentos = new ConcurrentDictionary<string, FixEngineMomento>();
            locksForHandlingStreamRead = new LockObjectsManager();
            sessionUpdatesLocks = new LockObjectsManager();
            sessionStatuses = new ConcurrentStack<string>();

            string[] msgTypes = File.ReadAllLines("fixMessageTypes.csv");
            GenerateDictionary(fixMsgTypes, msgTypes);

            string[] fixTags = File.ReadAllLines("fixTagValuePair.csv");
            GenerateDictionary(fixTagValues, fixTags);

            LoadFixTagValueConfigurations();
        }

        public void LoadFIXEngines() { }

        public void LoadFIXEnginesAndSessions()
        {
            foreach (var dictkey in fixEnginesDB.Keys.ToArray())
            {
                var redisIPWithPortAndDB = LineSplitterWithAString(dictkey);
                var muxer = RedisConnectorHelper.GetConnection(redisIPWithPortAndDB[0]);
                int db = fixEnginesDB[dictkey];

                string CacheKeyEvent = "__keyevent@" + db + "__:*";
                var client = muxer.GetDatabase(db);
                var engine = client.HashGetAll("Engine");
                if (engine.Length == 0)
                {
                    fixEnginesDB.Remove(dictkey);
                    continue;
                }

                string key = engine[0].Value;
                var engine_data = client.HashGetAll(key);
                FIXEngine FIXEngine = CreateFixEngine(db, engine_data);

                // creating sockets for already present engines present in config files and redis both after engine is created in memory
                FixEngineSocketHandler(FIXEngine);

                ReadMessages(client, FIXEngine, existingMessage);
                GetSessionsForEngine(muxer, db, client, FIXEngine);
                SubscribeAndFaliureCallback(FIXEngine, muxer, CacheKeyEvent); // Since its the start of the day, SubscribeAndFaliureCallback() is called after reading existing messages.
            }
            //fixEngines.Add(new FIXEngine() { engineID = "ATS_FIX", engineName = "ATS_FIX", ipAddress = "192.168.0.1", port = 4044 });
        }

        public void LoadFixTagValueConfigurations()
        {
            using (var context = new FIXMonitorContext())
            {
                var allFixTagValueConfigurations = context.FixTagValues.ToList();

                EmailNotifier.fixTagValueConfigurations = allFixTagValueConfigurations;
            }
        }

        public void SubscribeFixEngineSocketUpdate(FIXEngine fixEngine)
        {
            SocketHandler socketHandler = fixEngineSocket.GetFixEngineSocket(fixEngine);

            IObservable<bool> data = socketHandler.GetStatus();
            data.Subscribe(updates =>
            {
                UpdateSessionStatus(updates, fixEngine); //TODO: Use Engine IP instead of configured ip
            });
        }

        public void FixEngineSocketHandler(FIXEngine fixEngine)
        {
            // creating socket handler object 
            fixEngineSocket.AddFixEngineSocket(fixEngine);
            SubscribeFixEngineSocketUpdate(fixEngine);
        }

        private string GetRedisStreamLastEntryId(IDatabase client)
        {
            return client.StreamInfo(redisStreamName).LastEntry.Id.ToString();
        }

        private void ReadMessages(IDatabase client, FIXEngine FIXEngine, bool existingOrRealTime)
        {
            lock (locksForHandlingStreamRead.GetLockObj(FIXEngine.engineName))
            {
                if (TimeStampUtility.CompareTimeStamps(streamLastReadTimeStamps[FIXEngine.engineName], GetRedisStreamLastEntryId(client)))
                {
                    var result = client.StreamRead(redisStreamName, streamLastReadTimeStamps[FIXEngine.engineName]);

                    ProcessAndSendMessages(result, "", FIXEngine, existingOrRealTime);
                    UpdateStreamPosition(result, FIXEngine.engineName);

                    // Send Acknowledgement
                    Task.Run(() =>
                    {
                        var streamValuesIds = result.Select(streamValue => streamValue.Id).ToArray();
                        if (streamValuesIds.Length > 0) client.StreamAcknowledgeAsync(redisStreamName, "", streamValuesIds);
                    });
                }
            }
        }

        private FIXEngine CreateFixEngine(int db, HashEntry[] engine_data)
        {
            FIXEngine fixEngine = new FIXEngine();
            var engine = proto.Engine.Default;

            var recieve = new FBE.proto.EngineModel();
            recieve.Attach(engine_data[0].Value);
            recieve.Deserialize(out engine);
            fixEngine = engine;

            fixEngine.fixSessions = new FixSessionKeyedCollection();

            fixEngines.Add(fixEngine);
            streamLastReadTimeStamps.Add(fixEngine.engineName, "0");
            streamLastReadTimeStamps.Add(fixEngine.engineName + ":Statuses", "0");
            hasSessionsBeenCreatedForAEngine.Add(fixEngine.engineName, false);
            return fixEngine;
        }

        private void GetSessionsForEngine(ConnectionMultiplexer muxer, int db, IDatabase client, FIXEngine FIXEngine)
        {
            var keys = muxer.GetServer(muxer.GetEndPoints().First()).Keys(db, "*-Config*");
            foreach (var item in keys)
            {
                string conId = item.ToString().Replace("-Config", "");
                string key = item.ToString().Replace("-Config", "-Status");
                var engine = fixEngines[FIXEngine.engineID];
                FIXSession session = engine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);

                if (session == null)
                {
                    session = createFixSession(client, FIXEngine, item, conId);
                    SetPreviousMessageUpdates(session, FIXEngine.engineID);
                }
                if (client.IsConnected(key))
                {
                    HashEntry[] state = HGetAllAsync(client, key);
                    if (state.Length > 0)
                    {
                        SessionUpdates(key, state, FIXEngine);
                    }
                }
            }

            hasSessionsBeenCreatedForAEngine[FIXEngine.engineName] = true;
        }

        private static HashEntry[] HGetAllAsync(IDatabase client, string key)
        {
            var hash = client.HashGetAllAsync(key);
            hash.Wait();
            var state = hash.Result;
            return state;
        }

        private FIXSession createFixSession(IDatabase client, FIXEngine FIXEngine, RedisKey item, string conId)
        {
            FIXSession session = new FIXSession();
            var sessionHash = HGetAllAsync(client, item.ToString());
            //Deserialize session from Redis

            var config = proto.Config.Default;
            var recieve = new FBE.proto.ConfigModel();
            recieve.Attach(sessionHash[0].Value);
            recieve.Deserialize(out config);

            session = config;
            if (FIXEngine.fixSessions.Contains(session.ConnectionID))
            {
                Logging.LogMessage(LOGTYPE.Debug, $"{session.ConnectionID} Already Exists");
                return null;
            }
            session.FixMessages = new List<FIXMessage>();
            FIXEngine.fixSessions.Add(session);
            session.ConnectionID = conId;
            return session;
        }

        private void SetPreviousMessageUpdates(FIXSession session, string engineID)
        {
            var _key = session.ConnectionID;
            if (sessionFixMessages.ContainsKey(_key))
            {
                session.FixMessages = sessionFixMessages[_key];
                sessionFixMessages.Remove(session.ConnectionID);
            }
        }

        public void GetFixMessagesFromRedis(ConnectionMultiplexer muxer, RedisChannel channel, RedisValue message, FIXEngine fixEngine)
        {
            Logging.LogMessage($"received {message} on {channel}");
            string key = message.ToString();
            if (key == "Statuses") return;
            int db = 0;
            if (fixEngines.Count > 0)
            {
                string dbkey = $"{fixEngine.engineID}";
                if (fixEnginesDB.ContainsKey(dbkey))
                    db = fixEnginesDB[dbkey];
                else
                    return;
            }
            else
            {
                return;
            }
            try
            {
                IDatabase client = muxer.GetDatabase(db);
                //StreamEntry[] messages;
                if (key == redisStreamName)
                {
                    ReadMessages(client, fixEngine, realTimeMessage);
                    return;
                }

                if (key.Contains("Config"))
                {
                    FIXSession session = createFixSession(muxer.GetDatabase(db), fixEngine, key, key.Replace("-Config", ""));
                    SendFixSessionUpdates(session, fixEngine.engineID, "insert");
                    //fixEngine.fixSessions.Add(session);
                }

                var hash = RedisCacheClient.getHashSet(muxer, key, db);
                hash.Wait();
                var result = hash.Result;

                if (key.Contains("Status"))
                {
                    if (channel.ToString().Contains("del"))
                    {
                        result = new HashEntry[0];
                    }
                    SessionUpdates(key, result, fixEngine);
                    return;
                }
                else if (channel.ToString().Contains("expire"))
                {
                    return;
                }
            }
            catch (Exception e)
            {
                LogException(e);
            }

            Logging.LogMessage("FINISHED READING...");
        }

        /*
        private void UpdateSessionStatuses(StreamEntry[] messages, FIXEngine _engine, string key)
        {
            string engineName = _engine.engineName;
            foreach (var message in messages)
            {
                UpdateStreamPosition(message, key, statusReadMessagesIDs);
                for (int i = 0; i < message.Values.Length; i++)
                {
                    var val = message.Values[i];
                    byte[] buffer = val.Value;

                    proto.Header header = proto.Header.Default;
                    var recieve = new FBE.proto.HeaderModel();
                    recieve.Attach(buffer);
                    recieve.Deserialize(out header);
                    if (header.Signature == Signature.FIXHUB)
                    {
                        var engine = fixEngines[_engine.engineID];
                        if (engine.fixSessions.Contains(header.ConnectionID))
                        {
                            var session = engine.fixSessions[header.ConnectionID];
                            session.InSeqNum = header.InSeqNum;
                            session.OutSeqNum = header.OutSeqNum;
                            session.Status = header.Status.ToString();
                            SendFixSessionUpdates(session, engine.engineID, "update");
                        }
                    }
                }
            }
        }
        */

        private void ProcessAndSendMessages(StreamEntry[] messages, string key, FIXEngine fixEngine, bool existingOrRealTime)
        {
            foreach (var message in messages)
            {
                for (int i = 0; i < message.Values.Length; i++)
                {
                    try
                    {
                        var val = message.Values[i];
                        byte[] buffer = val.Value;
                        proto.Body body = proto.Body.Default;
                        var recieve = new FBE.proto.BodyModel();
                        recieve.Attach(buffer);
                        bool proceed = true; // recieve.Verify(); -> TODO:: Not Working as expected... 
                        if (proceed)
                        {
                            //if (recieve.Verify())
                            //{
                            recieve.Deserialize(out body);
                            //} else
                            //{
                            //    body = Body.Default;
                            //}
                            
                            FIXMessage fixMessage = body;
                            var _key = body.ConnectionID;

                            if (existingOrRealTime && hasSessionsBeenCreatedForAEngine[fixEngine.engineName])
                            {
                                SendFixMessageUpdates(fixMessage, fixEngine.engineID, _key, true);
                                bool isStored = StoreRealTimeFixMessage(fixEngine, fixMessage, _key);
                                if (!isStored) Logging.LogMessage("Cannot store realtime fixMessage Message");
                                Task.Run(() =>
                                {
                                    FixMessageLog.FixMessageLogger(_key, fixEngine, fixMessage);
                                });
                            }
                            else
                            {
                                if (!sessionFixMessages.ContainsKey(_key)) sessionFixMessages.Add(_key, new List<FIXMessage>());
                                sessionFixMessages[_key].Add(fixMessage);
                            }
                        }
                        else
                        {
                            Logging.LogMessage("ERROR : " + message.Id);
                            Logging.LogMessage("ERROR : " + recieve.model.ToString());
                        }
                    }
                    catch (Exception e)
                    {
                        LogException(e);
                    }
                }
            }
        }

        public bool StoreRealTimeFixMessage(FIXEngine fixEngine, FIXMessage fixMessage, string _key)
        {
            FIXSession fixSession = null;

            var engine = fixEngines.FirstOrDefault(x => x.engineName == fixEngine.engineName);

            if (engine != null) fixSession = engine.fixSessions.FirstOrDefault(s => s.ConnectionID == _key);

            if (fixSession != null)
            {
                fixSession.FixMessages.Add(fixMessage);
                return true;
            }

            return false;
        }

        public void LoadFIXSessions()
        {
            //Populate fixSessions list with FIX sessions.
            string fixMessage = "8=FIX.4.4|9=75|35=A|34=1092|49=TESTBUY1|52=20180920-18:24:59.643|56=TESTSELL1|98=0|108=60|10=178|";
            FIXMessage fixMessageObj = new FIXMessage();
            fixMessageObj.fixMessage = fixMessage;
            fixMessageObj.keyValuePair = FIXMessage.ParseAndStoreFixMessage(fixMessage);
            fixMessageObj.messageType = fixMsgTypes[FIXMessage.GetFixTagValue(fixMessage, "35")];
            fixMessageObj.sendingTime = FIXMessage.GetFixTagValue(fixMessage, "52");

            string fixMessage1 = "8=FIX.4.4|9=75|35=A|34=1092|49=TESTBUY1|52=20190920-18:24:59.643|56=TESTSELL1|98=0|108=60|10=178|";
            FIXMessage fixMessageObj1 = new FIXMessage();
            fixMessageObj1.fixMessage = fixMessage1;
            fixMessageObj1.keyValuePair = FIXMessage.ParseAndStoreFixMessage(fixMessage1);
            fixMessageObj1.messageType = fixMsgTypes[FIXMessage.GetFixTagValue(fixMessage1, "35")];
            fixMessageObj1.sendingTime = FIXMessage.GetFixTagValue(fixMessage1, "52");


            fixEngines[0].fixSessions.Add(new FIXSession() { ConnectionID = "t-trader_VLCY", SenderCompID = "Trader", TargetCompID = "VLCY", InSeqNum = 3, OutSeqNum = 2, LastUpdated = DateTime.Now, Status = "connected", FixMessages = new List<FIXMessage>() { fixMessageObj } });

            fixEngines[0].fixSessions.Add(new FIXSession() { ConnectionID = "trader_VLCY-t", SenderCompID = "Trader", TargetCompID = "VLCY", InSeqNum = 5, OutSeqNum = 48, LastUpdated = DateTime.Now, Status = "disconnected", FixMessages = new List<FIXMessage>() { fixMessageObj1 } });
        }

        public int GetDBForEngine(FIXSession fixSession, FIXEngine engine)
        {
            return engine.redisDB;
        }

        public bool ConnectFixSessionAsync(FIXSession fixSession)
        {
            var success = PerformGivenActionToRedis(fixSession, proto.Action.CONNECT);
            return success;
        }

        public bool DisconnectFixSession(FIXSession fixSession)
        {
            var success = PerformGivenActionToRedis(fixSession, proto.Action.DISCONNECT);
            return success;
        }

        public bool PerformGivenActionToRedis(FIXSession fixSession, proto.Action action)
        {
            bool isVerified = false;
            try
            {
                var engine = fixEngines.FirstOrDefault(x => x.fixSessions.FirstOrDefault(y => y.IPAddress + ":" + y.Port == fixSession.IPAddress + ":" + fixSession.Port && y.ConnectionID == fixSession.ConnectionID) != null);
                var ip = engine.redisIpAddress + ":" + engine.redisIpPort;

                var muxer = RedisConnectorHelper.GetConnection($"{engine.redisIpAddress}:{engine.redisIpPort}");
                int db = fixEnginesDB[$"{engine.engineID}"];
                var database = muxer.GetDatabase(db);
                //If the data is not consistent then we will read the data first and then update the data ... 

                Header header = new Header()
                {
                    Action = action,
                    ConnectionID = fixSession.ConnectionID,
                    InSeqNum = fixSession.InSeqNum,
                    OutSeqNum = fixSession.OutSeqNum,
                    SenderID = fixSession.SenderCompID,
                    TargetID = fixSession.TargetCompID,
                    Signature = Signature.FIXMONITOR,
                    //Status = proto.MessageStatus.Default.GetMessageStatus(fixSession.Status)
                };

                FBE.proto.HeaderModel headerModel = new FBE.proto.HeaderModel();
                headerModel.Serialize(header);
                isVerified = headerModel.Verify();

                if (isVerified)
                {
                    database.StreamAddAsync("Statuses", action.ToString(), headerModel.Buffer.Data).Wait();
                    //database.HashSetAsync(fixSession.ConnectionID + "-Status", "Status" , headerModel.Buffer.Data).Wait();
                }
            }
            catch (Exception e)
            {
                LogException(e);
            }

            return isVerified;
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_TARGET_SEQUENCE);
            return isCompleted;
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            fixSession.InSeqNum = 0;
            fixSession.OutSeqNum = 0;
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_TARGET_SEQUENCE);
            return isCompleted;
        }

        private void GetStatusUpdates(FIXSession fixSession, bool success)
        {
            if (success)
            {
                Thread thread = new Thread(
                    unused => isConnected(fixSession.ConnectionID, fixSession)
                    );
                thread.Start();
            }
        }

        public string GetFixMessagesAsync(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            List<FIXMessage> ordersTemp = fixEngines[fixEngineID].fixSessions[fixSessionConnectionID].FixMessages;
            if (!string.IsNullOrEmpty(dataSourceLoadOptions))
            {
                try
                {
                    DataSourceLoadOptions loadOptions = JsonConvert.DeserializeObject<DataSourceLoadOptions>(dataSourceLoadOptions);
                    return JsonConvert.SerializeObject(DataSourceLoader.Load(ordersTemp, loadOptions));

                }
                catch (Exception e)
                {
                    Logging.LogMessage(LOGTYPE.Fatal, e.Message + e.StackTrace);
                }
            }

            return JsonConvert.SerializeObject(DataSourceLoader.Load(ordersTemp, null));
        }

        public List<FIXMessage> GetFixMessagesHavingAnyConfiguredFixTagValuePair(string sessionID)
        {
            if (string.IsNullOrEmpty(sessionID))
                return new List<FIXMessage>();

            return fixMessagesContainingConfiguredFixTagValuePair.ContainsKey(sessionID) ? fixMessagesContainingConfiguredFixTagValuePair[sessionID] : new List<FIXMessage>();
        }

        /*
        private void FilteringEachListOnSeparateThreadFromThreadPool(List<FIXMessage> fixMessagesList, FixmessageRejects reject)
        {
            var fixMsgList = fixMessagesList.ToList();

            foreach(var msg in fixMsgList)
            {
                foreach(var desc in msg.keyValuePair)
                {
                    if (desc.Item1.Equals(reject.FixTag) && desc.Item3.Equals(reject.FixValue))
                    {
                        fixMessagesList.Remove(msg);
                        break;
                    }  
                }
            }
        }

        public void FiltrationOfFixMessagesWithRespectToCurrentConfiguredTagValuePairs(int id)
        {
            List<Task> tasks = new List<Task>();

            using (var context = new FIXMonitorContext())
            {
                var reject = context.FixmessageRejects.FirstOrDefault(r => r.Id == id);

                if(reject != null)
                {
                    foreach(var msgList in fixMessagesContainingConfiguredFixTagValuePair.Values)
                    {
                        var task = Task.Run(() =>
                        {
                            FilteringEachListOnSeparateThreadFromThreadPool(msgList, reject);
                        });

                        tasks.Add(task);
                    }
                }
            }

            Task.WaitAll(tasks.ToArray());
        }
        */

        private void SetScheduler()
        {
            foreach (var engine in fixEngines)
            {
                foreach (var session in engine.fixSessions)
                {
                    using (var context = new FIXMonitorContext())
                    {
                        var sessionInfo = context.Sessions.FirstOrDefault(s => s.SessionId == session.ConnectionID);

                        if (sessionInfo != null && sessionInfo.EmailStatus && TimeConverter.CompareTimeDifference(sessionInfo.ScheduleTime.TimeOfDay, DateTime.Now.TimeOfDay) > 0)
                        {
                            var totalTimeInMilliseconds = TimeConverter.GetTimeInMilliseconds(sessionInfo.ScheduleTime.TimeOfDay - DateTime.Now.TimeOfDay) + TimeConverter.GetTimeInMilliseconds(sessionInfo.Timeout);

                            emailNotifier = new EmailNotifier(totalTimeInMilliseconds, session, sessionInfo);
                        }
                    }
                }
            }
        }

        public FixEnginesKeyedCollection GetFixEngines()
        {
            return fixEngines.GetClone(); // Cloning exclude copying fixMessages since we need to reduce the payload in order to fasten the rendering time of UI
        }

        public FIXEngine GetFixEngine(string engineID)
        {

            return fixEngines.Contains(engineID) ? fixEngines[engineID].GetClone() : null;
        }

        public FIXEngine ConnectToFixEngine(FIXEngine fixEngine)
        {
            fixEngine.engineID = GetKey(fixEngine);
            fixEngine.fixSessions = new FixSessionKeyedCollection();
            if (fixEngines.Contains(fixEngine.engineID))
            {
                throw new Exception($"Engine Already Exists with DB : {fixEngine.redisDB}");
            }

            fixEngines.Add(fixEngine);

            ConnectionMultiplexer muxer = null;
            try
            {
                muxer = RedisConnectorHelper.GetConnection($"{fixEngine.redisIpAddress}:{fixEngine.redisIpPort}");

                // It will create socket connection when new engine is added from client side

                // creating socket right after it creates redis connection
                FixEngineSocketHandler(fixEngine);
            }
            catch (Exception e)
            {
                fixEngines.Remove(fixEngine);
                throw e;
            }

            int db = GetDBForEngine(null, fixEngine);
            if (db == -1)
            {
                fixEngines.Remove(fixEngine);
                throw new Exception($"DB not found");
            }

            string CacheKeyEvent = "__keyevent@" + db + "__:*";
            var key = $"{fixEngine.engineID}";
            if (!fixEnginesDB.ContainsKey(key))
            {
                fixEnginesDB.Add(key, db);
                PersistFixEngineConfig(fixEngine, false);
            }
            else
            {
                fixEnginesDB[key] = db;
                fixEngines.Remove(fixEngine);
            }

            //var engineHash = FIXEngine.getHashFromObject(fixEngine);
            try
            {
                proto.Engine engine = fixEngine;
                var send = new FBE.proto.EngineModel();
                send.Serialize(engine);

                HashEntry[] engineHash = new HashEntry[1];
                engineHash[0] = new HashEntry("Engine", send.Buffer.Data);

                var client = muxer.GetDatabase(db);

                var setEngine = client.HashSetAsync(fixEngine.engineName.ToUpper(), engineHash);
                setEngine.Wait();

                var setEngines = client.HashSetAsync("Engine", fixEngine.engineID, fixEngine.engineName.ToUpper());
                setEngines.Wait();

                if (streamLastReadTimeStamps.ContainsKey(fixEngine.engineName))
                {
                    streamLastReadTimeStamps[fixEngine.engineName] = "0";
                    streamLastReadTimeStamps[fixEngine.engineName + ":Statuses"] = "0";
                }
                else
                {
                    streamLastReadTimeStamps.Add(fixEngine.engineName, "0");
                    streamLastReadTimeStamps.Add(fixEngine.engineName + ":Statuses", "0");
                }

                hasSessionsBeenCreatedForAEngine.Add(fixEngine.engineName, false);

                SubscribeAndFaliureCallback(fixEngine, muxer, CacheKeyEvent); // Since its the middle of the day, SubscribeAndFaliureCallback() is called before reading existing messages as to not miss any real-time message while reading the existing one's.
                ReadMessages(client, fixEngine, existingMessage);
                GetSessionsForEngine(muxer, db, client, fixEngine);
            }
            catch (Exception e)
            {
                LogException(e);
            }

            return fixEngine;
        }

        private void PersistFixEngineConfig(FIXEngine fixEngine = null, bool getConfigFromSessionDbDict = true)
        {
            if (getConfigFromSessionDbDict)
                File.Delete(fixEngineRedisConfigFilePath);

            StreamWriter sw = new StreamWriter(fixEngineRedisConfigFilePath, true);

            if (getConfigFromSessionDbDict)
            {
                foreach (var key in fixEnginesDB.Keys)
                {
                    sw.WriteLine(key);
                }
            }

            else
                sw.WriteLine(fixEngine.engineID);

            sw.Flush();
            sw.Close();
        }

        private void SubscribeAndFaliureCallback(FIXEngine fixEngine, ConnectionMultiplexer muxer, string CacheKeyEvent)
        {
            SubscribeToKeyEvent(fixEngine, muxer, CacheKeyEvent);
            muxer.ConnectionFailed += (sender, args) =>
            {
                Logging.LogMessage("Lost Connection with REDIS");
                //muxer.GetSubscriber().UnsubscribeAsync(CacheKeyEvent).Wait();
                //Logging.LogMessage("{0} Un Subscribed");
            };
            muxer.ConnectionRestored += (sender, args) =>
            {
                Logging.LogMessage("Connection Restored with REDIS");
                //SubscribeToKeyEvent(fixEngine, muxer, CacheKeyEvent);
                Logging.LogMessage("{0} Subscribed");
            };
        }

        private void SubscribeToKeyEvent(FIXEngine fixEngine, ConnectionMultiplexer muxer, string CacheKeyEvent)
        {
            muxer.GetSubscriber().Subscribe(CacheKeyEvent,
                                                        (channel, message) => GetFixMessagesFromRedis(muxer, channel, message, fixEngine));

        }

        public FIXEngine DisconnectToFixEngine(FIXEngine fixEngine)
        {
            var key = $"{fixEngine.engineID}";

            var columns = LineSplitterWithAString(key);
            var nestColumns = LineSplitter(columns.First());

            fixEngine.redisIpPort = nestColumns.Last();
            fixEngine.redisDB = Convert.ToInt32(columns.Last());

            var engine = fixEngines.SingleOrDefault(x => x.redisIpAddress == fixEngine.redisIpAddress && x.redisIpPort == fixEngine.redisIpPort && x.redisDB == fixEngine.redisDB);

            if (engine != null)
            {
                fixEngines.Remove(engine);
            }
            if (fixEnginesDB.ContainsKey(key))
            {
                var db = fixEnginesDB[key];
                fixEnginesDB.Remove(key);
                PersistFixEngineConfig(engine, true);
                string CacheKeyEvent = "__keyevent@" + db + "__:*";
                var muxer = RedisConnectorHelper.GetConnection($"{fixEngine.redisIpAddress}:{fixEngine.redisIpPort}");
                muxer.GetSubscriber().Unsubscribe(CacheKeyEvent);
                muxer.GetDatabase(engine.redisDB).HashDeleteAsync("Engine", engine.engineID).Wait();
            }

            hasSessionsBeenCreatedForAEngine.Remove(engine.engineName);

            return fixEngine;
        }

        public FIXSession ConnectToFixSession(string engineID, FIXSession fixSession)
        {
            fixSession.ConnectionID = fixSession.SenderCompID + "-" + fixSession.TargetCompID;
            fixSession.Status = "disconnected";
            fixSession.LastUpdated = DateTime.Now;

            if (String.IsNullOrEmpty(fixSession.BackUpIPAddress))
            {
                fixSession.BackUpIPAddress = fixSession.IPAddress;
            }

            if (fixSession.BackUpPort == 0)
            {
                fixSession.BackUpPort = fixSession.Port;
            }

            var engine = fixEngines[engineID];
            int db = fixEnginesDB[$"{engine.engineID}"];

            proto.Config session = (proto.Config)fixSession;
            FBE.proto.ConfigModel configModel = new FBE.proto.ConfigModel();
            configModel.Serialize(session);

            var sessionHash = new HashEntry[1] { new HashEntry(fixSession.ConnectionID + "-Config", configModel.Buffer.Data) }; //FIXSession.getHashFromObject(fixSession);

            var muxer = RedisConnectorHelper.GetConnection($"{engine.redisIpAddress}:{engine.redisIpPort}");
            var client = muxer.GetDatabase(db);
            //client.HashSet(fixSession.ConnectionID + "-Config", sessionHash);
            client.HashSet("Sessions-To-Be-Added", sessionHash);

            //SendFixSessionUpdates(fixSession, engineID, "insert");

            return fixSession;
        }

        public Task SendSampleFixMessages()
        {
            Random random = new Random();
            while (true)
            {
                if (fixEngines == null || fixEngines.Count() == 0)
                {
                    Thread.Sleep(1000);
                    continue;
                }
                try
                {
                    int i = random.Next(0, fixEngines.Count() - 1);
                    int j = random.Next(0, fixEngines[i].fixSessions.Count() - 1);
                    string engineID = fixEngines[i].engineID;
                    string sessionID = fixEngines[i].fixSessions[j].ConnectionID;
                    FIXSession session = new FIXSession() { AutoConnect = false, SenderCompID = i.ToString(), TargetCompID = j.ToString(), ConnectionID = Guid.NewGuid().ToString(), LastUpdated = DateTime.Now };
                    FIXMessage message = new FIXMessage() { fixMessage = "", keyValuePair = new List<Tuple<string, string, string>>(), messageType = "", sendingTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.zzz") };
                    fixEngines[engineID].fixSessions[j].FixMessages.Add(message);
                    fixEngines[engineID].fixSessions.Add(session);
                    Logging.LogMessage($"Fix Message sent for EngineID { engineID } SessionID: { sessionID }");
                    SendFixMessageUpdates(message, engineID, sessionID, false);
                    SendFixSessionUpdates(session, engineID, "insert");
                }
                catch (Exception e)
                {
                    Logging.LogMessage("Exception in SendSampleFixMessage, message: " + e.Message + ", StackTrace" + e.StackTrace);
                }
                Thread.Sleep(60000);
            }
        }

        public void SendFixSessionUpdates(FIXSession fixSession, string engineID, string updateType)
        {
            if (fixSession == null) return;
            observable.SendFixSessionUpdate(fixSession, engineID, updateType);
        }

        public void SendFixMessageUpdates(FIXMessage fixMessage, string engineID, string sessionID, bool isRealTime)
        {
            observable.SendFixMessageUpdate(fixMessage, engineID, sessionID);
            Task.Run(() =>
            {
                CheckForConfiguredFixTagValuePair(fixMessage, engineID, sessionID, isRealTime);
            });
        }

        public void SendFixSessionStatusMessage(string fixSessionStatusMessage)
        {
            observable.SendFixSessionStatusMessage(fixSessionStatusMessage, "fixSessionStatusMessage");
        }

        public void CheckForConfiguredFixTagValuePair(FIXMessage fixMessage, string engineID, string sessionID, bool isRealTime)
        {
            var IsUpdateSentToObservers = false;

            foreach (var desc in fixMessage.keyValuePair)
            {
                if (!isRealTime && IsUpdateSentToObservers)
                    break;

                var res = EmailNotifier.fixTagValueConfigurations.FirstOrDefault(f => f.FixTag.Equals(desc.Item1) && f.FixValue.Equals(desc.Item3));

                if (res != null)
                {
                    if (isRealTime && res.EmailStatus)
                    {
                        emailNotifier = new EmailNotifier(sessionID, res).SendEmailForFIXMessageReject();
                        Logging.LogMessage(LOGTYPE.Info, $"Fix Message Email sent for Configured Tag/Value Pair {desc.Item1}/{desc.Item3} in it");
                    }

                    if (!IsUpdateSentToObservers)
                    {
                        StoreFixMessagesContainingConfiguredFixTagValuePair(fixMessage, sessionID);
                        observable.SendFixMessageContainingConfiguredFixTagValuePairUpdate(fixMessage, engineID, sessionID);
                        IsUpdateSentToObservers = true;
                    }
                }
            }
        }

        public void StoreFixMessagesContainingConfiguredFixTagValuePair(FIXMessage fixMessage, string sessionID)
        {
            if (fixMessagesContainingConfiguredFixTagValuePair.ContainsKey(sessionID))
                fixMessagesContainingConfiguredFixTagValuePair[sessionID].Add(fixMessage);

            else
                fixMessagesContainingConfiguredFixTagValuePair.Add(sessionID, new List<FIXMessage>() { fixMessage });
        }

        public IEnumerable<FIXSessionsConnectivityStatus> GetFixSessionsConnectivityStatus()
        {
            var fixSessionsConnectivityStatusList = fixEngines.SelectMany(engine => engine.fixSessions.Select(session => new FIXSessionsConnectivityStatus()
            {
                engineID = engine.engineID,
                engineName = engine.engineName,
                ConnectionID = session.ConnectionID,
                Status = session.Status
            }));

            return fixSessionsConnectivityStatusList;
        }

        public void InvokeSessionUpdates(string engineName)
        {
            var fixEngine = fixEngines.FirstOrDefault(engine => engine.engineName.Equals(engineName, StringComparison.OrdinalIgnoreCase));

            foreach(var session in fixEngine.fixSessions)
            {
                SendFixSessionUpdates(session, fixEngine.engineID, "insert");
            }
        }

        public FixSessionKeyedCollection GetFixSession(string FixEngineID)
        {
            return fixEngines[FixEngineID].fixSessions;
        }

        public FIXMessage getObjectFromFixMessage(string fixMessage)
        {
            FIXMessage fixMessageObj = new FIXMessage();
            fixMessageObj.fixMessage = fixMessage;
            //Logging.LogMessage("FIX MESSAGE : " + fixMessage);
            fixMessageObj.keyValuePair = FIXMessage.ParseAndStoreFixMessage(fixMessage);
            fixMessageObj.messageType = fixMsgTypes[FIXMessage.GetFixTagValue(fixMessage, "35")];
            fixMessageObj.sendingTime = FIXMessage.GetFixTagValue(fixMessage, "52");
            //Logging.LogMessage("SENDING TIME : " + fixMessageObj.sendingTime);

            return fixMessageObj;
        }

        public void isConnected(string key, FIXSession fixSession)
        {
            try
            {
                string subkey = "Status";
                var engine = fixEngines.FirstOrDefault(x => x.fixSessions.SingleOrDefault(y => y.ConnectionID == key) != null);
                key = key + "-" + subkey;
                var muxer = RedisConnectorHelper.GetConnection($"{engine.redisIpAddress}:{engine.redisIpPort}");
                int db = fixEnginesDB[$"{engine.engineID}"];
                var hash = RedisCacheClient.getHashSet(muxer, key, db);
                hash.Wait();
                var result = hash.Result;
                SessionUpdates(key, result, engine);
            }
            catch (Exception e)
            {
                LogException(e);
            }

        }

        public string CreateFixSessionUpdateMessage(string fixEngineName, string fixSessionID, string fixSessionStatus)
        {
            string sessionStatusMessage = $"[{fixEngineName}] {fixSessionID} is {fixSessionStatus} at {DateTime.Now}";

            return sessionStatusMessage;
        }

        public void PushFixSessionStatusMessage(string fixSessionStatusMessage)
        {

            Task.Run(() =>
            {
                try
                {
                    sessionStatuses.Push(fixSessionStatusMessage);
                    Logging.LogMessage(LOGTYPE.Info, $"fix session status message stored  {fixSessionStatusMessage}");
                }
                catch (Exception ex)
                {
                    Logging.LogMessage(LOGTYPE.Fatal, $"Cant Add fix session status message {fixSessionStatusMessage} in store {ex.Message}");
                }
            });
        }

        public void SendSessionConnectionStatusMessage(string ConnectionID, string sessionConnection, string statusConnection, FIXEngine fixEngine)
        {
            if (statusConnection != sessionConnection)
            {
                string fixSessionStatusMessage = CreateFixSessionUpdateMessage(fixEngine.engineName, ConnectionID, statusConnection);

                PushFixSessionStatusMessage(fixSessionStatusMessage);

                try
                {
                    SendFixSessionStatusMessage(fixSessionStatusMessage);
                }
                catch (Exception ex)
                {

                    Logging.LogMessage(LOGTYPE.Fatal, $"Cant Send fix session status message {fixSessionStatusMessage} to client. Exception : {ex.Message}");
                }
            }
        }

        // key: redis key of session id 
        public void SessionUpdates(string key, HashEntry[] result, FIXEngine fixEngine)
        {
            var status = proto.Header.Default;
            var recieve = new FBE.proto.HeaderModel();
            recieve.Attach(result[0].Value);
            recieve.Deserialize(out status);
            //Logging.LogMessage(LOGTYPE.Debug, status.ToString());
            //Dictionary<string, string> hashmap = new Dictionary<string, string>();
            //foreach (var i in result)
            //{
            //    hashmap.Add(i.Name, i.Value);
            //}
            //if (hashmap.Keys.Count == 0)
            //{
            //    hashmap.Add("InSeq", "0");
            //    hashmap.Add("OutSeq", "0");
            //    hashmap.Add("Status", "unavailable");
            //}
            //var inSeq = hashmap["InSeq"].Split('\0');
            //var outSeq = hashmap["OutSeq"].Split('\0');
            //var status = hashmap["Status"].Split('\0');

            string conId = key.Replace("-Status", "");
            try
            {
                var engine = fixEngines.SingleOrDefault(x => x.engineID == fixEngine.engineID);

                // returns fix sessions in specified engine id 

                var session = GetFixSession(engine.engineID).SingleOrDefault(x => x.ConnectionID == conId);
                if (session != null)
                {
                    bool sendEmail;
                    lock (sessionUpdatesLocks.GetLockObj(session.ConnectionID))
                    {
                        var statusConnection = status.Status.ToString();
                        var sessionConnection = session.Status;

                        SendSessionConnectionStatusMessage(session.ConnectionID, sessionConnection, statusConnection, fixEngine);

                        sendEmail = session.Status != status.Status.ToString();
                        session.InSeqNum = status.InSeqNum;
                        session.OutSeqNum = status.OutSeqNum;
                        session.Status = status.Status.ToString();
                        session.LastUpdated = DateTime.Now;

                        SendFixSessionUpdates(session, engine.engineID, "update");

                        if (sendEmail)
                        {
                            SendFixSessionUpdates(session, engine.engineID, "update_status_in_fix_sessions_dropdown");

                            using (var context = new FIXMonitorContext())
                            {
                                var sessionInfo = context.Sessions.FirstOrDefault(s => s.SessionId == conId);

                                if (sessionInfo != null && sessionInfo.EmailStatus) // If email alert has been enabled for a particular session
                                {
                                    if (!EmailNotifier.emailTimer.ContainsKey(sessionInfo.SessionId) && session.Status.Equals("Connected", StringComparison.OrdinalIgnoreCase))
                                        emailNotifier = new EmailNotifier(conId, session.Status, sessionInfo).SendEmail();

                                    else if (EmailNotifier.emailTimer.ContainsKey(sessionInfo.SessionId) && session.Status.Equals("Connected", StringComparison.OrdinalIgnoreCase))
                                    {
                                        System.Timers.Timer timer;
                                        EmailNotifier.emailTimer.TryGetValue(sessionInfo.SessionId, out timer);
                                        timer.Stop();
                                        timer.Dispose();

                                        EmailNotifier.emailTimer.Remove(sessionInfo.SessionId);

                                        if ((bool)sessionInfo.Recurring)
                                        {
                                            if (EmailNotifier.recurringEmailsCount[sessionInfo.SessionId] > 0)
                                                emailNotifier = new EmailNotifier(conId, session.Status, sessionInfo).SendEmail();

                                            EmailNotifier.recurringEmailsCount.Remove(sessionInfo.SessionId);
                                        }

                                    }

                                    else if (!EmailNotifier.emailTimer.ContainsKey(sessionInfo.SessionId) && session.Status.Equals("Disconnected", StringComparison.OrdinalIgnoreCase))
                                    {
                                        int intervalInMilliseconds = TimeConverter.GetTimeInMilliseconds(sessionInfo.Timeout);

                                        emailNotifier = new EmailNotifier(intervalInMilliseconds, conId, session.Status, sessionInfo);
                                        EmailNotifier.emailTimer.Add(sessionInfo.SessionId, emailNotifier.getTimerInstance());
                                    }

                                    //emailHandler.SendEmail(conId, session.Status, sessionInfo);

                                    Console.WriteLine($"FixHandler -> SessionUpdates -> {session.ConnectionID} -> {session.Status}");
                                    Console.WriteLine("Custom Email Settings used");
                                }

                                else
                                {
                                    //Console.WriteLine($"Email Alert for Session {conId} is disabled");

                                    // Default Email Setting if an individual session is not configured.

                                    Console.WriteLine($"FixHandler -> SessionUpdates -> {session.ConnectionID} -> {session.Status}");

                                    emailNotifier = new EmailNotifier(conId, session.Status, new Sessions() { SessionId = session.ConnectionID }).SendEmail();

                                    Console.WriteLine("Default Email Settings used");
                                }
                            }

                            Logging.LogMessage(LOGTYPE.Info, $"Fix Session Email sent for SessionID: {session.ConnectionID}");
                        }
                    }
                }
                Logging.LogMessage($"Fix Session Update sent for EngineID: { engine.engineID } SessionID: { session.ConnectionID }");
            }
            catch (Exception e)
            {
                LogException(e);
            }
        }

        public IEnumerable<string> GetSessionStatusMessage()
        {
            return sessionStatuses.ToList();
        }

        private void GenerateDictionary(Dictionary<string, string> dic, string[] lines)
        {
            for (int i = 0; i < lines.Length; i++)
            {
                var data = lines[i].Split(',');
                dic.Add(data[0], data[1]);
            }
        }

        private void UpdateStreamPosition(StreamEntry[] streamValues, string engineName)
        {
            if (streamValues.Length > 0)
            {
                streamLastReadTimeStamps[engineName] = streamValues[streamValues.Length - 1].Id.ToString();
            }
        }  

        private void UpdateSessionStatus(bool isConnected, FIXEngine fixEngine)
        {
            try
            {
                if (!isConnected)
                {
                    // creating multiple engineMomento object

                    if (!fixEngineMomentos.ContainsKey(fixEngine.engineID)) fixEngineMomentos.TryAdd(fixEngine.engineID, new FixEngineMomento());

                    FixEngineMomento engineMomento;
                    var isEngineMomentoExists = fixEngineMomentos.TryGetValue(fixEngine.engineID, out engineMomento);

                    if (isEngineMomentoExists)
                    {
                        engineMomento.SetState(fixEngine);
                    }
                    else
                    {
                        Logging.LogMessage(LOGTYPE.Error, "Could not get engine Momento object");
                    }

                    //if (isConnected)
                    //{
                    //    var _state = engineMomento.GetState();
                    //    if (_state == null) return;
                    //    fixEngines = _state;
                    //}

                    bool statusInFixSessionsDropdownUpdate = false;

                    // only sending updates to individual fix engine
                    foreach (var session in fixEngine.fixSessions)
                    {
                        if (session.Status != "UNAVAILABLE")
                        {
                            session.Status = "UNAVAILABLE";
                            SendFixSessionUpdates(session, fixEngine.engineID, "update");
                            statusInFixSessionsDropdownUpdate = true;
                        }
                    }

                    if (statusInFixSessionsDropdownUpdate)
                    {
                        SendFixSessionUpdates(fixEngine.fixSessions[0], fixEngine.engineID, "update_status_in_fix_sessions_dropdown");
                    }
                }
            }
            catch (Exception e)
            {
                LogException(e);
            }
        }

        private static void LogException(Exception e)
        {
            Logging.LogMessage(LOGTYPE.Error, "Exception : " + e.Message);
            Logging.LogMessage(LOGTYPE.Error, "StackTrace : " + e.StackTrace);
            if (e.InnerException != null)
            {
                Logging.LogMessage(LOGTYPE.Error, "Inner Exception : " + e.InnerException.Message);
                Logging.LogMessage(LOGTYPE.Error, "StackTrace Inner Exception : " + e.InnerException.StackTrace);
            }
        }

        private string GetKey(FIXEngine fixEngine)
        {
            return $"{fixEngine.redisIpAddress}:{fixEngine.redisIpPort}::{fixEngine.redisDB}";
        }
    }
}
