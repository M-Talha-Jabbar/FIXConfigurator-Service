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
using FIXMonitorBusinessLogicLayer.IComparers;
using static FIXMonitorServer.FIXHubCommunicator;
using FBE;
using CoreLogging;
using proto;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    public class FixHandler : IFixHandler
    {
        //private FIXHubCommunicator.FIXHubCommunicatorClient fixGrpcClient;
        private FixEnginesKeyedCollection fixEngines;
        private Dictionary<string, int> fixEnginesDB;
        private Dictionary<string, Channel> fixEnginesChannels;
        private Dictionary<string, List<int>> session_dbs;
        private Dictionary<string, FIXHubCommunicatorClient> fixEnginesGrcpClients;
        public static Dictionary<string, string> fixMsgTypes = new Dictionary<string, string>();
        public static Dictionary<string, string> fixTagValues = new Dictionary<string, string>();
        Observable observable = new Observable();

        private readonly bool sendSampleFixUpdate = Convert.ToBoolean(System.Configuration.ConfigurationManager.AppSettings["sendSampleFixUpdate"].ToString());
        private readonly string redisStreamName = System.Configuration.ConfigurationManager.AppSettings["redisStreamName"].ToString();
        private readonly string statusStreamName = "Statuses";
        //Messages Stream Attributes
        private Dictionary<string, long> streamLastReadTimeStamps;
        private long streamLastPosition = 0;
        private List<RedisValue> readMessagesIDs;

        //Status Stream Attributes
        private long statusStreamLastPosition = 0;
        private List<RedisValue> statusReadMessagesIDs;

        private Dictionary<string, List<FIXMessage>> sessionFixMessages;

        public FixHandler()
        {
            fixEngines = new FixEnginesKeyedCollection();
            fixEnginesDB = new Dictionary<string, int>();
            fixEnginesGrcpClients = new Dictionary<string, FIXHubCommunicatorClient>();
            fixEnginesChannels = new Dictionary<string, Channel>();
            session_dbs = new Dictionary<string, List<int>>();
            readMessagesIDs = new List<RedisValue>();
            statusReadMessagesIDs = new List<RedisValue>();
            streamLastReadTimeStamps = new Dictionary<string, long>();
            sessionFixMessages = new Dictionary<string, List<FIXMessage>>();
            //ConnectToGRPCServer();
            string[] msgTypes = File.ReadAllLines("fixMessageTypes.csv");
            GenerateDictionary(fixMsgTypes, msgTypes);

            string[] fixTags = File.ReadAllLines("fixTagValuePair.csv");
            GenerateDictionary(fixTagValues, fixTags);

            //Persistence Work -- 
            if (File.Exists("redisConfigAndDB.txt"))
            {
                List<string> data = File.ReadAllLines("redisConfigAndDB.txt").ToList();
                foreach (var row in data)
                {
                    var columns = row.Split(':');
                    var db = Int32.Parse(columns[1]);
                    if (session_dbs.ContainsKey(columns[0]))
                    {
                        if(!session_dbs[columns[0]].Contains(db))
                            session_dbs[columns[0]].Add(db);
                    }
                    else
                    {
                        session_dbs.Add(columns[0], new List<int>() { db });
                    }
                }
            }

            if (sendSampleFixUpdate)
            {
                Task.Run(async () => await SendSampleFixMessages());
            }
            
            //------------------------------------------------------------------------------
            
            //Thread GRPCStatusThread = new Thread(new ThreadStart(CheckGRPCStatusAsync));
            //GRPCStatusThread.Start();
                                /* TODO : CheckFixHubStatus thread */
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

        private void CheckGRPCStatusAsync()
        {

            FIXHubCommunicatorClient client = null;
            bool isServerListening = true;
            var fixEngine = new FIXEngine();
            int index = 0;
            while (true)
            {
                int length = fixEngines.Count;
                if (fixEngines.Count > 0)
                {
                    fixEngine = fixEngines[index++ % length];
                    if (fixEngine.fixSessions.Count > 0)
                    {
                        FIXSession fixSession = fixEngine.fixSessions[0];
                        client = ConnectToGRPCServer(fixSession);
                    }
                }

                if (client == null)
                {
                    Thread.Sleep(1000);
                    continue;
                }

                try
                {
                    var reply = client.Check(
                      new HealthCheckRequest { Service = "Status" });
                    if (reply.Status == HealthCheckResponse.Types.ServingStatus.Serving)
                    {
                        //Console.WriteLine("Server is Serving");
                        if (!isServerListening)
                        {
                            var muxer = RedisConnectorHelper.GetConnection(fixEngine.redisIpAddress);
                            var db = fixEnginesDB[$"{fixEngine.engineID}"];
                            var clientDb = muxer.GetDatabase(db);
                            GetSessionsForEngine(muxer, db, clientDb, fixEngine);
                            isServerListening = true;
                            Thread.Sleep(1000);
                        }
                    }
                    else if (reply.Status == FIXMonitorServer.HealthCheckResponse.Types.ServingStatus.NotServing)
                    {
                        Console.WriteLine("Server Disconnected the client");
                        Thread.Sleep(1000);
                        break;
                    }



                }
                catch (Exception e)
                {

                    Console.WriteLine("Server Unvailable..." + e.Message);
                    if (e.Message.Contains("failed to connect to all addresses") && isServerListening)
                    {

                        isServerListening = false;
                        //foreach (var engines in fixEngines)
                        //{
                        foreach (var sessions in fixEngine.fixSessions)
                        {
                            if (sessions.Status.ToLower() != "unavailable")
                            {
                                sessions.Status = "unavailable";
                                sessions.LastUpdated = DateTime.Now;
                                SendFixSessionUpdates(sessions, fixEngine.engineID, "update");
                            }
                        }
                        Thread.Sleep(1000);
                    }//}
                    //make All session Unavailable at Frontend Logic Goes Here.
                }
                //Thread.Sleep(1000);
            }
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
                    muxer.GetSubscriber().Subscribe(CacheKeyEvent,
                                (channel, message) => GetFixMessagesFromRedis(muxer, channel, message, FIXEngine));

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
                FIXSession session = FIXEngine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);

                if (session == null)
                {
                    session = createFixSession(client, FIXEngine, item, conId);
                    SendFixSessionUpdates(session, FIXEngine.engineID, "insert");
                }
                HashEntry[] state = HGetAllAsync(client, key);
                SessionUpdates(key, state, FIXEngine);
                SendPreviousMessageUpdates(session, FIXEngine.engineID);
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

            //FIXSession.setObjectFromHash(session, sessionHash);

            session = config;
            //SendPreviousMessageUpdates(session);
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
            Console.WriteLine($"received {message} on {channel}");
            string key = message.ToString();
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
                    client.StreamAcknowledgeAsync(key, "", readMessagesIDs.ToArray()).Wait();
                    readMessagesIDs.Clear();
                    return;
                }
                else if (key == statusStreamName)
                {
                    //var streamKey = fixEngine.engineName + ":Statuses";
                    //GetStreamMessages(muxer, streamKey, key, db, out client, out messages);
                    //UpdateSessionStatuses(messages, fixEngine, streamKey);
                    //client.StreamAcknowledgeAsync(key, "", statusReadMessagesIDs.ToArray()).Wait();
                    //statusReadMessagesIDs.Clear();
                    return;
                }
                else
                {
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

            }
            catch (Exception e)
            {
                Logging.LogMessage($"ERROR1 : {e.Message}");
            }
            //}
            //var val = new RedisCacheClient().getHashSetItem(muxer, new RedisKey("myhash3"), new RedisValue("field6"));
            Console.WriteLine("FINISHED READING...");
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
                            session.InSecNum = header.InSecNum;
                            session.OutSecNum = header.OutSecNum;
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
                    var val = message.Values[i];
                    byte[] buffer = val.Value;
                    proto.Body body = proto.Body.Default;
                    var recieve = new FBE.proto.BodyModel();
                    recieve.Attach(buffer);
                    recieve.Deserialize(out body);
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


            fixEngines[0].fixSessions.Add(new FIXSession() { ConnectionID = "t-trader_VLCY", SenderCompID = "Trader", TargetCompID = "VLCY", InSecNum = 3, OutSecNum = 2, LastUpdated = DateTime.Now, Status = "connected", FixMessages = new List<FIXMessage>() { fixMessageObj } });

            fixEngines[0].fixSessions.Add(new FIXSession() { ConnectionID = "trader_VLCY-t", SenderCompID = "Trader", TargetCompID = "VLCY", InSecNum = 5, OutSecNum = 48, LastUpdated = DateTime.Now, Status = "disconnected", FixMessages = new List<FIXMessage>() { fixMessageObj1 } });
        }

        public FIXHubCommunicatorClient ConnectToGRPCServer(FIXSession fixSession)
        {
            //Channel will be changed as per the server
            //Channel channel = new Channel("127.0.0.1:30051", ChannelCredentials.Insecure);
            string ip = "";
            try
            {

                var engine = fixEngines.FirstOrDefault(x => x.fixSessions.FirstOrDefault(y => y.IPAddress + ":" + y.Port == fixSession.IPAddress + ":" + fixSession.Port && y.ConnectionID == fixSession.ConnectionID) != null);
                ip = engine.redisIpAddress + ":" + engine.redisIpPort;
                if (fixEnginesGrcpClients.ContainsKey(ip))
                {
                    return fixEnginesGrcpClients[ip];
                }
                else
                {
                    Channel channel = new Channel(ip, ChannelCredentials.Insecure);
                    //channel.ConnectAsync();
                    var client = new FIXHubCommunicatorClient(channel);
                    fixEnginesGrcpClients.Add(ip, client);
                    fixEnginesChannels.Add(ip, channel);
                    return client;
                }
                //CoreLogging.Logging.LogMessage($"GRPC Server IP Address : { engine.ipAddress } Port : { engine.port }");

            }
            catch (Exception e)
            {
                //CoreLogging.Logging.LogMessage(CoreLogging.LOGTYPE.Error, e.Message);
                CoreLogging.Logging.LogMessage($"GRPC Server ERROR { e.Message }");
                return null;
                //ip = "192.168.0.43:50051";
            }


            //fixGrpcClient = client;

        }

        public int GetDBForEngine(FIXSession fixSession, FIXEngine engine)
        {
            return engine.redisDB;

            //FIXHubCommunicatorClient fixGrpcClient = null;

            //if (fixSession == null)
            //{
            //    string ip = engine.ipAddress + ":" + engine.port;

            //    if (fixEnginesGrcpClients.ContainsKey(ip))
            //    {

            //        fixGrpcClient = fixEnginesGrcpClients[ip];
            //    }
            //    else
            //    {
            //        Channel channel = new Channel(ip, ChannelCredentials.Insecure);
            //        //channel.ConnectAsync();
            //        var client = new FIXHubCommunicator.FIXHubCommunicatorClient(channel);
            //        fixEnginesGrcpClients.Add(ip, client);
            //        fixEnginesChannels.Add(ip, channel);
            //        fixGrpcClient = client;
            //    }
            //}
            //else
            //{

            //    fixGrpcClient = ConnectToGRPCServer(fixSession);
            //}
            //try
            //{
            //    var task = Task.Run(async () => fixGrpcClient.EngineDB(
            //    new GetEngineDbRequest
            //    {

            //    }
            //    ));

            //    task.Wait();
            //    var result = task.Result;
            //    Console.WriteLine("MESSAGE : " + result.Db);
            //    return result.Db;
            //}
            //catch (Exception e)
            //{
            //    CoreLogging.Logging.LogMessage($"Attemp to get DB failed with message {e.InnerException.Message}");
            //    string ip = $"{engine.ipAddress}:{engine.port}";
            //    fixEnginesGrcpClients.Remove(ip);
            //    return -1;
            //}

        }

        public bool ConnectFixSessionAsync(FIXSession fixSession)
        {
            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();
            thread.Join();
            return PerformGivenActionToRedis(fixSession, proto.Action.CONNECT);
        }

        public bool DisconnectFixSession(FIXSession fixSession)
        {
            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();
            thread.Join();
            return PerformGivenActionToRedis(fixSession, proto.Action.DISCONNECT);
        }

        public bool PerformGivenActionToRedis(FIXSession fixSession, proto.Action action)
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
                InSecNum = fixSession.InSecNum,
                OutSecNum = fixSession.OutSecNum,
                SenderID = fixSession.SenderCompID,
                TargetID = fixSession.TargetCompID,
                Signature = Signature.FIXMONITOR
            };

            FBE.proto.HeaderModel headerModel = new FBE.proto.HeaderModel();
            headerModel.Serialize(header);
            bool isVerified = headerModel.Verify();

            if(isVerified)
            {
                database.StreamAddAsync("Statuses", fixSession.ConnectionID, headerModel.Buffer.Data).Wait();
                //database.HashSetAsync(fixSession.ConnectionID + "-Status", "Status" , headerModel.Buffer.Data).Wait();
            }

            return isVerified;
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();
            thread.Join();
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.SET_TARGET_SEQUENCE);
            return isCompleted;
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            fixSession.InSecNum = 0;
            fixSession.OutSecNum = 0;
            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();
            thread.Join();
            bool isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_SENDER_SEQUENCE);
            isCompleted = PerformGivenActionToRedis(fixSession, proto.Action.RESET_TARGET_SEQUENCE);
            return isCompleted;
        }

        public List<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID)
        {
            return fixEngines[fixEngineID].fixSessions[fixSessionConnectionID].FixMessages;
        }

        public FixEnginesKeyedCollection GetFixEngines()
        {
            return fixEngines;
        }

        public FIXEngine ConnectToFixEngine(FIXEngine fixEngine)
        {
            //Request to connect

            //Add in keyed collection
            fixEngine.engineID = Guid.NewGuid().ToString();
            fixEngine.fixSessions = new FixSessionKeyedCollection();
            fixEngines.Add(fixEngine);

            // Add to Grpc and Request A DB No. for now i am assuming 3
            var muxer = RedisConnectorHelper.GetConnection($"{fixEngine.redisIpAddress}:{fixEngine.redisIpPort}");
            int db = GetDBForEngine(null, fixEngine);
            if (db == -1)
            {
                fixEngines.Remove(fixEngine);
                //throw new Exception($"cant connect to IP : {fixEngine.redisIpAddress} and Port : {fixEngine.redisIpPort}");
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
                throw new Exception($"Engine Already Exists with DB : {fixEngine.redisDB}");
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

                Thread thread = new Thread(
                    unused => GetSessionsForEngine(muxer, db, client, fixEngine)
                    );
                thread.Start();
                //GetSessionsForEngine(muxer, db, client, fixEngine);
                muxer.GetSubscriber().Subscribe(CacheKeyEvent,
                                (channel, message) => GetFixMessagesFromRedis(muxer, channel, message, fixEngine));
            }
            catch(Exception e)
            {
                Console.WriteLine("Exception : " + e.Message);
                if(e.InnerException != null)
                {
                    Console.WriteLine("Inner Exception : " + e.InnerException.Message);
                }
            }
            return fixEngine;
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
            //Console.WriteLine("FIX MESSAGE : " + fixMessage);
            fixMessageObj.keyValuePair = FIXMessage.ParseAndStoreFixMessage(fixMessage);
            fixMessageObj.messageType = fixMsgTypes[FIXMessage.GetFixTagValue(fixMessage, "35")];
            fixMessageObj.sendingTime = FIXMessage.GetFixTagValue(fixMessage, "52");
            //Console.WriteLine("SENDING TIME : " + fixMessageObj.sendingTime);

            return fixMessageObj;
        }

        public void isConnected(string key, FIXSession fixSession)
        {
            return;
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


                var engine = fixEngines.SingleOrDefault(x => x.redisIpAddress == fixEngine.redisIpAddress && x.redisIpPort == fixEngine.redisIpPort);
                var session = GetFixSession(engine.engineID).SingleOrDefault(x => x.ConnectionID == conId);

                session.InSecNum = status.InSecNum;
                session.OutSecNum = status.OutSecNum;
                session.Status = status.Status.ToString();
                
                session.LastUpdated = DateTime.Now;
                SendFixSessionUpdates(session, engine.engineID, "update");
                CoreLogging.Logging.LogMessage($"Fix Session Update sent for EngineID { engine.engineID } SessionID: { session.ConnectionID }");
            }
            catch (Exception e)
            {
                CoreLogging.Logging.LogMessage($"ERROR2  { e.Message }");
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
    }
}
