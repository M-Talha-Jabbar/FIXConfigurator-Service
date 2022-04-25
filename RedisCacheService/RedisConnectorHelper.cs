using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace RedisCacheService
{
    public class RedisConnectorHelper
    {
        static RedisConnectorHelper()
        {
            muxers = new Dictionary<string, ConnectionMultiplexer>();
        }

        private static Lazy<ConnectionMultiplexer> lazyConnection;
        private static Dictionary<string, ConnectionMultiplexer> muxers;

        public static ConnectionMultiplexer GetConnection(string ipAddress)
        {
            try
            {
                string ip = ipAddress.Split(':')[0];
                if (muxers.ContainsKey(ip))
                {
                    return muxers[ip];
                }
                var lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
                {
                    var configOption = new ConfigurationOptions();
                    configOption.EndPoints.Add(ipAddress);
                    configOption.AllowAdmin = true;
                    var muxer = ConnectionMultiplexer.Connect(configOption);
                    muxer.GetServer(muxer.GetEndPoints().Single())
                         .ConfigSet("notify-keyspace-events", "KEA");
                    return muxer;
                });
                var val = lazyConnection.Value;
                muxers.Add(ip, val);
                return val;
            }
            catch (Exception e)
            {
                throw new Exception($"Cant Connect To Redis Server on {ipAddress}");
            }
        }

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;
            }

        }
    }
}
