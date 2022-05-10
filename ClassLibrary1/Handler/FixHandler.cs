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
using FIXMonitorServer;
using RedisCacheService;
using StackExchange.Redis;
using CoreLogging;
using proto;
using System.Reactive.Subjects;
using FIXMonitorBusinessLogicLayer.Momentos;
using FBE.proto;
using DevExtreme.AspNet.Mvc;
using DevExtreme.AspNet.Data;
using Newtonsoft.Json;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class FixHandler : IFixHandler
    {
        private FixEnginesKeyedCollection fixEngines;
        private Dictionary<string, int> fixEnginesDB;
        private Dictionary<string, Channel> fixEnginesChannels;
        private Dictionary<string, List<int>> session_dbs;
        public static Dictionary<string, string> fixMsgTypes = new Dictionary<string, string>();
        public static Dictionary<string, string> fixTagValues = new Dictionary<string, string>();
        Observable observable = new Observable();
        private readonly bool sendSampleFixUpdate = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings["sendSampleFixUpdate"].ToString());
        private readonly string redisStreamName = System.Configuration.ConfigurationManager.AppSettings["redisStreamName"].ToString();
        //Messages Stream Attributes
        private Dictionary<string, long> streamLastReadTimeStamps;
        private long streamLastPosition = 0;
        private List<RedisValue> readMessagesIDs;

        //Status Stream Attributes
        private long statusStreamLastPosition = 0;
        private List<RedisValue> statusReadMessagesIDs;

        private Dictionary<string, List<FIXMessage>> sessionFixMessages;

        private FixEngineMomento engineMomento;

        public FixHandler()
        {
            Initializers();
            //Persistence Work -- 
            EnginePersistence();

            if (sendSampleFixUpdate)
            {
                Task.Run(async () => await SendSampleFixMessages());
            }

            //------------------------------------------------------------------------------
            /* HACK : FixHub Status */
            IObservable<bool> data = SocketHandler.GetStatus();
            data.Subscribe(updates => {
                    UpdateSessionStatus(updates); //TODO: Use Engine IP instead of configured ip
            });
            //------------------------------------------------------------------------------
            LoadFIXEnginesAndSessions();

            //Save The updated configuration to the file 

            StreamWriter sw = new StreamWriter("redisConfigAndDB.txt");
            foreach (var key in session_dbs.Keys)
            {
                foreach (var db in session_dbs[key])
                {
                    sw.WriteLine($"{key}:{db}");
                }
            }
            sw.Flush();
            sw.Close();

        }

        private void EnginePersistence()
        {
            if (File.Exists("redisConfigAndDB.txt"))
            {
                List<string> data = File.ReadAllLines("redisConfigAndDB.txt").ToList();
                foreach (var row in data)
                {
                    var columns = row.Split(':');
                    var db = Int32.Parse(columns[1]);
                    if (session_dbs.ContainsKey(columns[0]))
                    {
                        if (!session_dbs[columns[0]].Contains(db))
                            session_dbs[columns[0]].Add(db);
                    }
                    else
                    {
                        session_dbs.Add(columns[0], new List<int>() { db });
                    }
                }
            }
        }

        private void Initializers()
        {
            fixEngines = new FixEnginesKeyedCollection();
            fixEnginesDB = new Dictionary<string, int>();
            fixEnginesChannels = new Dictionary<string, Channel>();
            session_dbs = new Dictionary<string, List<int>>();
            readMessagesIDs = new List<RedisValue>();
            statusReadMessagesIDs = new List<RedisValue>();
            streamLastReadTimeStamps = new Dictionary<string, long>();
            sessionFixMessages = new Dictionary<string, List<FIXMessage>>();
            string[] msgTypes = File.ReadAllLines("fixMessageTypes.csv");
            GenerateDictionary(fixMsgTypes, msgTypes);

            string[] fixTags = File.ReadAllLines("fixTagValuePair.csv");
            GenerateDictionary(fixTagValues, fixTags);

            engineMomento = new FixEngineMomento();
        }

        public void LoadFIXEngines() { }

        public void LoadFIXEnginesAndSessions()
        {
            foreach (var dictkey in session_dbs.Keys)
            {
                var muxer = RedisConnectorHelper.GetConnection(dictkey);
                int[] dbs = session_dbs[dictkey].ToArray();

                for (int i = 0; i < dbs.Length; i++)
                {
                    int db = dbs[i];
                    string CacheKeyEvent = "__keyevent@" + db + "__:*";
                    var client = muxer.GetDatabase(db);
                    var engine = client.HashGetAll("Engine");
                    if (engine.Length == 0)
                    {
                        session_dbs[dictkey].Remove(db);
                        continue;
                    }

                    string key = engine[0].Value;
                    var engine_data = client.HashGetAll(key);
                    FIXEngine FIXEngine = CreateFixEngine(db, engine_data);
                    ReadAllExistingFixMessages(client, FIXEngine);

                    GetSessionsForEngine(muxer, db, client, FIXEngine);
                    SubscribeAndFaliureCallback(FIXEngine, muxer, CacheKeyEvent);

                }
            }
            //fixEngines.Add(new FIXEngine() { engineID = "ATS_FIX", engineName = "ATS_FIX", ipAddress = "192.168.0.1", port = 4044 });
        }

        private void ReadAllExistingFixMessages(IDatabase client, FIXEngine FIXEngine)
        {
            var stream = client.StreamReadAsync(redisStreamName, streamLastReadTimeStamps[FIXEngine.engineName]);
            stream.Wait();
            var result = stream.Result;
            ProcessAndSendMessages(result, "", FIXEngine, false);
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
            
            fixEnginesDB.Add($"{fixEngine.engineID}", db);
            fixEngines.Add(fixEngine);
            streamLastReadTimeStamps.Add(fixEngine.engineName, 0);
            streamLastReadTimeStamps.Add(fixEngine.engineName + ":Statuses", 0); 
            return fixEngine;
        }

        private void GetSessionsForEngine(ConnectionMultiplexer muxer, int db, IDatabase client, FIXEngine FIXEngine)
        {
            var keys = muxer.GetServer(muxer.GetEndPoints().First()).Keys(db, "*-Config*");
            foreach (var item in keys)
            {
                string conId = item.ToString().Replace("-Config", "");
                string key = item.ToString().Replace("-Config", "-Status");
                //FIXEngine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);
                var engine = fixEngines[FIXEngine.engineID];
                FIXSession session = engine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);

                if (session == null)
                {
                    session = createFixSession(client, FIXEngine, item, conId);
                    SendFixSessionUpdates(session, FIXEngine.engineID, "insert");
                    SendPreviousMessageUpdates(session, FIXEngine.engineID);
                }
                if (client.IsConnected(key))
                {
                    HashEntry[] state = HGetAllAsync(client, key);
                    if (state.Length > 0)
                    {
                        SessionUpdates(key, state, FIXEngine);
                        SendPreviousMessageUpdates(session, FIXEngine.engineID);
                    }
                }
            }
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
            if (FIXEngine.fixSessions.Contains(session.ConnectionID)) {
                Logging.LogMessage(LOGTYPE.Debug, $"{session.ConnectionID} Already Exists");
                return null;
            }
            session.FixMessages = new List<FIXMessage>();
            FIXEngine.fixSessions.Add(session);
            session.ConnectionID = conId;
            return session;
        }

        private void SendPreviousMessageUpdates(FIXSession session, string engineID)
        {
            var _key = session.ConnectionID;
            if (sessionFixMessages.ContainsKey(_key))
            {
                session.FixMessages = sessionFixMessages[_key];
                sessionFixMessages.Remove(session.ConnectionID);
                foreach (var message in session.FixMessages)
                {
                    SendFixMessageUpdates(message, engineID, session.ConnectionID);
                }
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
                if(fixEnginesDB.ContainsKey(dbkey))
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
                IDatabase client;
                StreamEntry[] messages;
                if (key == redisStreamName)
                {
                    GetStreamMessages(muxer, fixEngine.engineName, key, db, out client, out messages);
                    ProcessAndSendMessages(messages, key, fixEngine);
                    if (readMessagesIDs.Count > 0)
                    {
                        client.StreamAcknowledgeAsync(key, "", readMessagesIDs.ToArray()).Wait();
                        readMessagesIDs.Clear();
                    }
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
            //}
            //var val = new RedisCacheClient().getHashSetItem(muxer, new RedisKey("myhash3"), new RedisValue("field6"));
            Logging.LogMessage("FINISHED READING...");
        }

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

        private void GetStreamMessages(ConnectionMultiplexer muxer, string engineName, string key, int db, out IDatabase client, out StreamEntry[] messages)
        {
            client = muxer.GetDatabase(db);
            var stream = client.StreamReadAsync(key, streamLastReadTimeStamps[engineName]);
            stream.Wait();
            messages = stream.Result;
        }

        private void ProcessAndSendMessages(StreamEntry[] messages, string key, FIXEngine fixEngine, bool IsSendMessage = true)
        {
            foreach (var message in messages)
            {
                UpdateStreamPosition(message,fixEngine.engineName, readMessagesIDs);
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
                            //var engine = GetFixEngines().SingleOrDefault(x => x.ipAddress == fixEngine.redisIpAddress && x.port == fixEngine.redisIpPort);
                            //var session = engine.fixSessions.Single(y => y.ConnectionID == key);
                            if (IsSendMessage)
                            {
                                observable.SendFixMessageUpdate(fixMessage, fixEngine.engineID, _key);
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
                    catch(Exception e)
                    {
                        LogException(e);
                    }

                }
            }
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
            GetStatusUpdates(fixSession, success);
            return success;
        }

        public bool DisconnectFixSession(FIXSession fixSession)
        {
            var success = PerformGivenActionToRedis(fixSession, proto.Action.DISCONNECT);
            GetStatusUpdates(fixSession, success);
            return success;
        }

        public bool PerformGivenActionToRedis(FIXSession fixSession, proto.Action action)
        {
            bool isVerified = false;
            try
            {
                var engine = fixEngines.FirstOrDefault(x => x.fixSessions.FirstOrDefault(y => y.IPAddress + ":" + y.Port == fixSession.IPAddress + ":" + fixSession.Port && y.ConnectionID == fixSession.ConnectionID) != null);
                var ip = engine.redisIpAddress + ":" + engine.redisIpPort;

                var muxer = RedisConnectorHelper.GetConnection(engine.redisIpAddress);
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
                    Signature = Signature.FIXMONITOR
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
            catch(Exception e)
            {
                LogException(e);
            }

            return isVerified;
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_TARGET_SEQUENCE);
            GetStatusUpdates(fixSession, isCompleted);
            return isCompleted;
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            fixSession.InSeqNum = 0;
            fixSession.OutSeqNum = 0;
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_TARGET_SEQUENCE);
            GetStatusUpdates(fixSession, isCompleted);
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

        //public List<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID)
        //{
        //    return fixEngines[fixEngineID].fixSessions[fixSessionConnectionID].FixMessages;
        //}

        public List<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions)
        {
            if (string.IsNullOrEmpty(fixEngineID) || string.IsNullOrEmpty(fixSessionConnectionID))
                return new List<FIXMessage>();

            List<FIXMessage> ordersTemp = fixEngines[fixEngineID].fixSessions[fixSessionConnectionID].FixMessages;
            if (!string.IsNullOrEmpty(dataSourceLoadOptions))
            {
                try
                {
                    DataSourceLoadOptions loadOptions = JsonConvert.DeserializeObject<DataSourceLoadOptions>(dataSourceLoadOptions);
                    return DataSourceLoader.Load(ordersTemp, loadOptions).data.OfType<FIXMessage>().ToList();
                }
                catch (Exception e)
                {

                }
            }
            return ordersTemp;

        }

        public FixEnginesKeyedCollection GetFixEngines()
        {
            return fixEngines;
        }

        public FIXEngine ConnectToFixEngine(FIXEngine fixEngine)
        {
            //Request to connect

            //Add in keyed collection
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
            //Save Db to File with respective ip
            

            string CacheKeyEvent = "__keyevent@" + db + "__:*";
            var key = $"{fixEngine.engineID}";
            if (!fixEnginesDB.ContainsKey(key))
            {
                fixEnginesDB.Add(key, db);
                StreamWriter sw = new StreamWriter("redisConfigAndDB.txt", true);
                sw.WriteLine($"{fixEngine.redisIpAddress}:{db}");
                sw.Flush();
                sw.Close();
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

                streamLastReadTimeStamps.Add(fixEngine.engineName, 0);
                streamLastReadTimeStamps.Add(fixEngine.engineName + ":Statuses", 0);

                Thread thread1 = new Thread(
                    unused => GetSessionsForEngine(muxer, db, client, fixEngine)
                    );
                thread1.Start();
                Thread thread2 = new Thread(
                    unused => ReadAllExistingFixMessages(client, fixEngine)
                    );
                thread2.Start();

                //GetSessionsForEngine(muxer, db, client, fixEngine);
                SubscribeAndFaliureCallback(fixEngine, muxer, CacheKeyEvent);

            }
            catch (Exception e)
            {
                LogException(e);
            }
            return fixEngine;
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
            //bool isRemoved = fixEngines.Remove(fixEngine);
            var engine = fixEngines.SingleOrDefault(x => x.redisIpAddress == fixEngine.redisIpAddress && x.redisIpPort == fixEngine.redisIpPort);
            if(engine != null)
            {
                fixEngines.Remove(engine);
            }
            if (fixEnginesDB.ContainsKey(key))
            {
                var db = fixEnginesDB[key];
                fixEnginesDB.Remove(key);
                string CacheKeyEvent = "__keyevent@" + db + "__:*";
                var muxer = RedisConnectorHelper.GetConnection(fixEngine.redisIpAddress);
                muxer.GetSubscriber().Unsubscribe(CacheKeyEvent);
                muxer.GetDatabase(engine.redisDB).HashDeleteAsync("Engine",engine.engineID).Wait();

            }
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
            engine.fixSessions.Add(fixSession);
            int db = fixEnginesDB[$"{engine.engineID}"];
            var sessionHash = new HashEntry[0]; //FIXSession.getHashFromObject(fixSession);
            
            var muxer = RedisConnectorHelper.GetConnection(engine.redisIpAddress);
            var client = muxer.GetDatabase(db);
            client.HashSet(fixSession.ConnectionID + "-Config", sessionHash);
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
                    CoreLogging.Logging.LogMessage($"Fix Message sent for EngineID { engineID } SessionID: { sessionID }");
                    SendFixMessageUpdates(message, engineID, sessionID);
                    SendFixSessionUpdates(session, engineID, "insert");
                }
                catch (Exception e)
                {
                    CoreLogging.Logging.LogMessage("Exception in SendSampleFixMessage, message: " + e.Message + ", StackTrace" + e.StackTrace);
                }
                Thread.Sleep(60000);
            }
        }

        public void SendFixSessionUpdates(FIXSession fixSession, string engineID, string updateType)
        {
            if (fixSession == null) return;
            observable.SendFixSessionUpdate(fixSession, engineID, updateType);
        }

        public void SendFixMessageUpdates(FIXMessage fixMessage, string engineID, string sessionID)
        {
            observable.SendFixMessageUpdate(fixMessage, engineID, sessionID);
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
            //return;
            try
            {
                string subkey = "Status";
                var engine = fixEngines.FirstOrDefault(x => x.fixSessions.SingleOrDefault(y => y.ConnectionID == key) != null);
                key = key + "-" + subkey;
                var muxer = RedisConnectorHelper.GetConnection(engine.redisIpAddress);
                int db = fixEnginesDB[$"{engine.engineID}"];
                var hash = RedisCacheClient.getHashSet(muxer, key, db);
                hash.Wait();
                var result = hash.Result;
                SessionUpdates(key, result, engine);
            }
            catch(Exception e)
            {
                LogException(e);
            }

        }

        public void SessionUpdates(string key, HashEntry[] result, FIXEngine fixEngine)
        {
            var status = proto.Header.Default;
            var recieve = new FBE.proto.HeaderModel();
            recieve.Attach(result[0].Value);
            recieve.Deserialize(out status);

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
                var session = GetFixSession(engine.engineID).SingleOrDefault(x => x.ConnectionID == conId);
                if (session != null)
                {
                    session.InSeqNum = status.InSeqNum;
                    session.OutSeqNum = status.OutSeqNum;
                    session.Status = status.Status.ToString();
                    session.LastUpdated = DateTime.Now;
                    SendFixSessionUpdates(session, engine.engineID, "update");
                }
                CoreLogging.Logging.LogMessage($"Fix Session Update sent for EngineID { engine.engineID } SessionID: { session.ConnectionID }");
            }
            catch (Exception e)
            {
                LogException(e);
            }
        }


        private void GenerateDictionary(Dictionary<string, string> dic, string[] lines)
        {
            for (int i = 0; i < lines.Length; i++)
            {
                var data = lines[i].Split(',');
                dic.Add(data[0], data[1]);
            }
        }

        private void UpdateStreamPosition(StreamEntry item, string engineName, List<RedisValue> readIDs)
        {
            string[] timestamp_seq = item.Id.ToString().Split('-');
            readIDs.Add(item.Id);
            string lastTimeStamp = timestamp_seq[0];
            streamLastReadTimeStamps[engineName] = long.Parse(lastTimeStamp);
        }

        private void UpdateSessionStatus(bool isConnected)
        {
            try
            {
                if (!isConnected)
                {
                    engineMomento.SetState(fixEngines);


                    //if (isConnected)
                    //{
                    //    var _state = engineMomento.GetState();
                    //    if (_state == null) return;
                    //    fixEngines = _state;
                    //}

                    foreach (var engine in fixEngines)
                    {
                        foreach (var session in engine.fixSessions)
                        {
                            if (!isConnected)
                                session.Status = "UNAVAILABLE";
                            SendFixSessionUpdates(session, engine.engineID, "update");
                        }
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
