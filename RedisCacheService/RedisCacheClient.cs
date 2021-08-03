using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisCacheService
{
    public class RedisCacheClient
    {
        private ConnectionMultiplexer _muxer;


        //public void SSHConnect()
        //{
        //    using (var client = new SshClient("192.168.0.86", "muhammadahmedmemon", "ahmed12345"))
        //    {
        //        client.Connect();
        //        var port = new ForwardedPortLocal("127.0.0.1", 42421, "127.0.0.1", 6379);
        //        client.AddForwardedPort(port);
        //        port.Exception += (sender, e) => Console.WriteLine(e.Exception.ToString());
        //        port.Start();
        //        using (var redisClient = new RedisClient("127.0.0.1", 42421))
        //        {
        //            var values = redisClient.As<string>();
        //            const string dansFord = "Dan's Ford Mustang";
        //            values.Store(dansFord);
        //            Console.WriteLine("Redis has " + values.GetAll().Count + " entries");
        //            values.GetAll().ToList().ForEach(Console.WriteLine);
        //        }
        //        Console.ReadLine();
        //        port.Stop();
        //        client.Disconnect();
        //    }
        //}
        //public ConnectionMultiplexer Connect()
        //{
        //    if (_muxer == null)
        //    {
        //        using var muxer = ConnectionMultiplexer.Connect("127.0.0.1,allowAdmin=true");
        //        var db = muxer.GetDatabase();
        //        if (db.StringSet("testKey", "testValue"))
        //        {
        //            var val = db.StringGet("testKey");

        //            Console.WriteLine(val);
        //        }

        //        muxer.GetServer(muxer.GetEndPoints().Single())
        //         .ConfigSet("notify-keyspace-events", "KEA");
        //        _muxer = muxer;
        //        Console.WriteLine("SUCCESSFULLY CONFIGURED");

        //    }
        //    return _muxer;
        //}

        public ISubscriber GetSubscriber(ConnectionMultiplexer muxer)
        {
            var subscriber = muxer.GetSubscriber();
            return subscriber;
        }

        public String getStringValue(ConnectionMultiplexer muxer, string key)
        {
            return muxer.GetDatabase().StringGet(key);
        }

        public static Task<RedisValue> getHashSetItem(ConnectionMultiplexer muxer, RedisKey key, RedisValue indexKey)
        {
            return muxer.GetDatabase().HashGetAsync(key, indexKey);
        }

        public static Task<HashEntry[]> getHashSet(ConnectionMultiplexer muxer, string key,int db = 3)
        {
            return muxer.GetDatabase(db).HashGetAllAsync(key);
        }

        public ISubscriber SubscribeKeySpace(ConnectionMultiplexer muxer, string ChatChannel = "__keyspace@0__:*")
        {
            var subscriber = muxer.GetSubscriber();
            subscriber.Subscribe(ChatChannel,
           (channel, message) =>
           {
               Console.WriteLine($"received {message} on {channel}");
           }
           );
            Console.WriteLine("SUCCESSFULLY SUBSCRIBED");
            return subscriber;
        }

        public ISubscriber SubscribeKeyEvent(ConnectionMultiplexer muxer, string ChatChannel = "__keyevent@0__:*")
        {
            var subscriber = muxer.GetSubscriber();
            subscriber.Subscribe(ChatChannel,
           (channel, message) => Console.WriteLine($"received {message} on {channel}"));
            return subscriber;
        }

        //public void ReadData()
        //{
        //    var cache = RedisConnectorHelper.Connection.GetDatabase();
        //    var devicesCount = 10000;
        //    for (int i = 0; i < devicesCount; i++)
        //    {
        //        var value = cache.StringGet($"Device_Status:{i}");
        //        Console.WriteLine($"Valor={value}");
        //    }
        //}

        //public void SaveBigData()
        //{
        //    var devicesCount = 10000;
        //    var rnd = new Random();
        //    var cache = RedisConnectorHelper.Connection.GetDatabase();

        //    for (int i = 1; i < devicesCount; i++)
        //    {
        //        var value = rnd.Next(0, 10000);
        //        cache.StringSet($"Device_Status:{i}", value);
        //    }
        //}
    }
}
