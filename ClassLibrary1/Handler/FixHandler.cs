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

        private Dictionary<string, long> engineStreamLastPosition;
        private Dictionary<string, List<FIXMessage>> sessionFixMessages;
        private long streamLastPosition = 0;
        private List<RedisValue> readMessagesIDs;

        public FixHandler()
        {
            test();

            fixEngines = new FixEnginesKeyedCollection();
            fixEnginesDB = new Dictionary<string, int>();
            fixEnginesGrcpClients = new Dictionary<string, FIXHubCommunicatorClient>();
            fixEnginesChannels = new Dictionary<string, Channel>();
            session_dbs = new Dictionary<string, List<int>>();
            readMessagesIDs = new List<RedisValue>();
            engineStreamLastPosition = new Dictionary<string, long>();
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
                            var db = fixEnginesDB[$"{fixEngine.ipAddress}:{fixEngine.port}"];
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
            var stream = client.StreamReadAsync(redisStreamName, engineStreamLastPosition[FIXEngine.engineName]);
            stream.Wait();
            var result = stream.Result;
            ProcessAndSendMessages(result, "", FIXEngine, false);
        }

        private FIXEngine CreateFixEngine(int db, HashEntry[] engine_data)
        {
            FIXEngine fixEngine = new FIXEngine();
            var engine = proto.Engine.Default;




            var recieve = new FBE.proto.EngineModel();
            recieve.Attach(engine_data[1].Value);
            recieve.Deserialize(out engine);
            fixEngine = engine;
            fixEngine.fixSessions = new FixSessionKeyedCollection();
            
            fixEnginesDB.Add($"{fixEngine.ipAddress}:{fixEngine.port}", db);
            fixEngines.Add(fixEngine);
            engineStreamLastPosition.Add(fixEngine.engineName, 0);
            return fixEngine;
        }

        private void GetSessionsForEngine(ConnectionMultiplexer muxer, int db, IDatabase client, FIXEngine FIXEngine)
        {
            var keys = muxer.GetServer(muxer.GetEndPoints().First()).Keys(db, "*-Config*");
            foreach (var item in keys)
            {
                string conId = item.ToString().Replace("-Config", "");
                string key = item.ToString().Replace("-Config", "-Status");
                FIXEngine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);
                FIXSession session = FIXEngine.fixSessions.FirstOrDefault(x => x.ConnectionID == conId);

                if (session == null)
                {
                    session = createFixSession(client, FIXEngine, item, conId);
                    SendFixSessionUpdates(session, FIXEngine.engineID, "insert");
                }
                var state = client.HashGetAll(key);
                SessionUpdates(key, state, FIXEngine);

            }
        }

        private static FIXSession createFixSession(IDatabase client, FIXEngine FIXEngine, RedisKey item, string conId)
        {
            FIXSession session = new FIXSession();
            var sessionHash = client.HashGetAll(item.ToString());
            //Deserialize session from Redis

            var config = proto.Config.Default;
            var recieve = new FBE.proto.ConfigModel();
            recieve.Attach(sessionHash[0].Value);
            recieve.Deserialize(out config);

            //FIXSession.setObjectFromHash(session, sessionHash);

            session = config;

            session.FixMessages = new List<FIXMessage>();
            FIXEngine.fixSessions.Add(session);
            session.ConnectionID = conId;
            return session;
        }

        public void GetFixMessagesFromRedis(ConnectionMultiplexer muxer, RedisChannel channel, RedisValue message, FIXEngine fixEngine)
        {
            Console.WriteLine($"received {message} on {channel}");
            string key = message.ToString();
            int db = 0;
            if (fixEngines.Count > 0)
            {
                string dbkey = $"{fixEngine.ipAddress}:{fixEngine.port}";
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

                if(key == redisStreamName)
                {
                    streamLastPosition = engineStreamLastPosition[fixEngine.engineName];
                    var client = muxer.GetDatabase(db);
                    var stream = client.StreamReadAsync(key, engineStreamLastPosition[fixEngine.engineName]);
                    stream.Wait();
                    var messages = stream.Result;
                    ProcessAndSendMessages(messages,key,fixEngine);
                    client.StreamAcknowledgeAsync(key, "",readMessagesIDs.ToArray()).Wait();
                    readMessagesIDs.Clear();
                    return;
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

                //Array.Sort(result, new FixMessageSorter(this));
                ////foreach (var item in hash.Result)
                ////{
                //if (result == null || result.Length == 0)
                //{
                //    return;
                //}
                //var item = result.Last();
                ////Console.WriteLine("Name: " + item.Name + " Value: " + item.Value);

                //var engine = GetFixEngines().SingleOrDefault(x => x.ipAddress == fixEngine.ipAddress && x.port == fixEngine.port);
                //var session = engine.fixSessions.Single(y => y.ConnectionID == key);
                //FIXMessage fixMessage = getObjectFromFixMessage(item.Value.ToString());
                //Console.WriteLine("TIME : " + fixMessage.sendingTime);
                //observable.SendFixMessageUpdate(fixMessage, engine.engineID, session.ConnectionID);
                //CoreLogging.Logging.LogMessage($"Fix Message sent for EngineID { engine.engineID } SessionID: { session.ConnectionID }");
            }
            catch (Exception e)
            {
                CoreLogging.Logging.LogMessage($"ERROR1 : {e.Message}");
            }
            //}
            //var val = new RedisCacheClient().getHashSetItem(muxer, new RedisKey("myhash3"), new RedisValue("field6"));
            Console.WriteLine("FINISHED READING...");
        }

        private void ProcessAndSendMessages(StreamEntry[] messages, string key, FIXEngine fixEngine, bool IsSendMessage = true)
        {
            foreach (var message in messages)
            {
                UpdateStreamPosition(message,fixEngine.engineName);
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
                    //var engine = GetFixEngines().SingleOrDefault(x => x.ipAddress == fixEngine.ipAddress && x.port == fixEngine.port);
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
                ip = engine.ipAddress + ":" + engine.port;
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


            FIXHubCommunicatorClient fixGrpcClient = null;

            if (fixSession == null)
            {
                string ip = engine.ipAddress + ":" + engine.port;

                if (fixEnginesGrcpClients.ContainsKey(ip))
                {

                    fixGrpcClient = fixEnginesGrcpClients[ip];
                }
                else
                {
                    Channel channel = new Channel(ip, ChannelCredentials.Insecure);
                    //channel.ConnectAsync();
                    var client = new FIXHubCommunicator.FIXHubCommunicatorClient(channel);
                    fixEnginesGrcpClients.Add(ip, client);
                    fixEnginesChannels.Add(ip, channel);
                    fixGrpcClient = client;
                }
            }
            else
            {

                fixGrpcClient = ConnectToGRPCServer(fixSession);
            }
            try
            {
                var task = Task.Run(async () => fixGrpcClient.EngineDB(
                new GetEngineDbRequest
                {

                }
                ));

                task.Wait();
                var result = task.Result;
                Console.WriteLine("MESSAGE : " + result.Db);
                return result.Db;
            }
            catch (Exception e)
            {
                CoreLogging.Logging.LogMessage($"Attemp to get DB failed with message {e.InnerException.Message}");
                string ip = $"{engine.ipAddress}:{engine.port}";
                fixEnginesGrcpClients.Remove(ip);
                return -1;
            }

        }

        public bool ConnectFixSessionAsync(FIXSession fixSession)
        {
            var fixGrpcClient = ConnectToGRPCServer(fixSession);

            //var task = Task.Run(async () => await fixGrpcClient.ConnectAsync(
            //    new ConnectRequest
            //    {
            //        SenderCompId = fixSession.SenderCompID,
            //        TargetCompId = fixSession.TargetCompID
            //    }
            //    ));
            //task.Wait();
            var task = fixGrpcClient.ConnectAsync(
                new ConnectRequest
                {
                    SenderCompId = fixSession.SenderCompID,
                    TargetCompId = fixSession.TargetCompID
                });
            //var result = task.Message;
            Console.WriteLine("CONNECT REQUEST SENT ...");

            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();

            //isConnected(fixSession.ConnectionID, fixSession);
            return true;
        }

        public bool DisconnectFixSession(FIXSession fixSession)
        {
            var fixGrpcClient = ConnectToGRPCServer(fixSession);
            //var task = Task.Run(async () => await fixGrpcClient.DisconnectAsync(
            //    new DisconnectRequest
            //    {
            //        SenderCompId = fixSession.SenderCompID,
            //        TargetCompId = fixSession.TargetCompID
            //    }
            //    ));
            var task = fixGrpcClient.DisconnectAsync(
                    new DisconnectRequest
                    {
                        SenderCompId = fixSession.SenderCompID,
                        TargetCompId = fixSession.TargetCompID
                    }
                );
            //var result = task.Result.Message;
            Console.WriteLine("DISCONNECT REQUEST SENT...");

            Thread thread = new Thread(
                unused => isConnected(fixSession.ConnectionID, fixSession)
                );
            thread.Start();

            //isConnected(fixSession.ConnectionID, fixSession);
            return true;
        }

        public bool SetSequenceNumber(FIXSession fixSession)
        {
            var fixGrpcClient = ConnectToGRPCServer(fixSession);
            var sender = fixGrpcClient.SetSenderSequence(
               new SetSenderSequenceRequest
               {
                   SenderCompId = fixSession.SenderCompID,
                   TargetCompId = fixSession.TargetCompID,
                   InSeq = 00 //fixSession.InSecNum
               }
               );
            Console.WriteLine(sender.Message);
            var target = fixGrpcClient.SetTargetSequence(
                new SetTargetSequenceRequest
                {
                    SenderCompId = fixSession.SenderCompID,
                    TargetCompId = fixSession.TargetCompID,
                    OutSeq = 00 //fixSession.OutSecNum
                }
                );
            Console.WriteLine(target.Message);
            return true;
        }

        public bool ResetSequenceNumber(FIXSession fixSession)
        {
            var fixGrpcClient = ConnectToGRPCServer(fixSession);
            var sender = fixGrpcClient.ResetSender(
                new SenderRequest
                {
                    SenderCompId = fixSession.SenderCompID,
                    TargetCompId = fixSession.TargetCompID
                }
                );
            Console.WriteLine(sender.Message);
            var target = fixGrpcClient.ResetTarget(
                new TargetRequest
                {
                    SenderCompId = fixSession.SenderCompID,
                    TargetCompId = fixSession.TargetCompID
                }
                );
            Console.WriteLine(target.Message);
            return true;
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
                throw new Exception($"cant connect to IP : {fixEngine.ipAddress} and Port : {fixEngine.port}");
            }
            //Save Db to File with respective ip
            

            string CacheKeyEvent = "__keyevent@" + db + "__:*";
            var key = $"{fixEngine.ipAddress}:{fixEngine.port}";
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
                throw new Exception($"Engine Already Exists with IP : {fixEngine.ipAddress} and Port : {fixEngine.port}");
            }

            //var engineHash = FIXEngine.getHashFromObject(fixEngine);
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
            Thread thread = new Thread(
                unused => GetSessionsForEngine(muxer, db, client, fixEngine)
                );
            thread.Start();
            //GetSessionsForEngine(muxer, db, client, fixEngine);
            muxer.GetSubscriber().Subscribe(CacheKeyEvent,
                            (channel, message) => GetFixMessagesFromRedis(muxer, channel, message, fixEngine));
            return fixEngine;
        }

        public FIXEngine DisconnectToFixEngine(FIXEngine fixEngine)
        {
            var key = $"{fixEngine.ipAddress}:{fixEngine.port}";
            //bool isRemoved = fixEngines.Remove(fixEngine);
            var engine = fixEngines.SingleOrDefault(x => x.ipAddress == fixEngine.ipAddress && x.port == fixEngine.port);
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
            }
            if (fixEnginesGrcpClients.ContainsKey(key))
            {
                fixEnginesGrcpClients.Remove(key);
                var channel = fixEnginesChannels[key];
                channel.ShutdownAsync();
                fixEnginesChannels.Remove(key);
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
            int db = fixEnginesDB[$"{engine.ipAddress}:{engine.port}"];
            var engineHash = new HashEntry[0]; //FIXSession.getHashFromObject(fixSession);
            var muxer = RedisConnectorHelper.GetConnection(engine.redisIpAddress);
            var client = muxer.GetDatabase(db);
            client.HashSet(fixSession.ConnectionID + "-Config", engineHash);
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
            string subkey = "Status";
            var engine = fixEngines.FirstOrDefault(x => x.fixSessions.SingleOrDefault(y => y.ConnectionID == key) != null);
            key = key + "-" + subkey;
            var muxer = RedisConnectorHelper.GetConnection(engine.redisIpAddress);
            int db = fixEnginesDB[$"{engine.ipAddress}:{engine.port}"];
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


                var engine = fixEngines.SingleOrDefault(x => x.ipAddress == fixEngine.ipAddress && x.port == fixEngine.port);
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

        private void UpdateStreamPosition(StreamEntry item, string engineName)
        {
            string[] timestamp_seq = item.Id.ToString().Split('-');
            readMessagesIDs.Add(item.Id);
            string lastTimeStamp = timestamp_seq[0];
            engineStreamLastPosition[engineName] = long.Parse(lastTimeStamp);
        }

        public void test()
        {
            try
            {


                FIXSession session = new FIXSession();
                var config = proto.Config.Default;
                config.SessionStart = (ulong)DateTime.Now.TimeOfDay.TotalMilliseconds;
                config.TargetCompID = "HELLO";
                config.Port = 900;
                config.Status = proto.MessageStatus.CONNECTED;
                //var recieve = new FBE.proto.ConfigModel();
                //recieve.Attach(sessionHash[0].Value);
                //recieve.Deserialize(out config);

                session = config;

                Console.WriteLine(config.ToString());
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }
}
