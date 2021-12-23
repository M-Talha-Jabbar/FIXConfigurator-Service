using FIXMonitorBusinessLogicLayer.KeyedCollections;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXEngine
    {
        public string engineID { get; set; } = Guid.NewGuid().ToString();
        public string engineName { get; set; }
        public string ipAddress { get; set; }
        public string redisIpAddress { get; set; }
        public string redisIpPort { get; set; }
        public string port { get; set; }
        public FixSessionKeyedCollection fixSessions { get; set; } = new FixSessionKeyedCollection();

        public static implicit operator FIXEngine(proto.Engine engine)
        {
            return new FIXEngine
            {
                engineID = engine.engineID,
                engineName = engine.engineName,
                ipAddress = engine.ipAddress,
                redisIpAddress = engine.redisIpAddress,
                redisIpPort = engine.redisIpPort,
                port = engine.port,
                fixSessions = new FixSessionKeyedCollection()
            };
        }

        public static implicit operator proto.Engine(FIXEngine engine)
        {
            return new proto.Engine
            {
                engineID = engine.engineID,
                engineName = engine.engineName,
                ipAddress = engine.ipAddress,
                redisIpAddress = engine.redisIpAddress,
                redisIpPort = engine.redisIpPort,
                port = engine.port
            };
        }

    }
}
