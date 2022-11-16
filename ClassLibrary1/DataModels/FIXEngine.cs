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
        public string redisIpAddress { get; set; }
        public string redisIpPort { get; set; }
        public int redisDB { get; set; }
        public string FIXEngineIpAddress { get; set; }
        public int FIXEngineIpPort { get; set; }

        public FixSessionKeyedCollection fixSessions { get; set; } = new FixSessionKeyedCollection();

        public static implicit operator FIXEngine(proto.Engine engine)
        {
            return new FIXEngine
            {
                engineID = engine.engineID,
                engineName = engine.engineName,
                redisIpAddress = engine.redisIpAddress,
                redisIpPort = engine.redisIpPort,
                redisDB = (int)engine.redisDB,
                fixSessions = new FixSessionKeyedCollection(),
                FIXEngineIpAddress = engine.FIXEngineIpAddress,
                FIXEngineIpPort = int.Parse(engine.FIXEngineIpPort)
            };
        }

        public static implicit operator proto.Engine(FIXEngine engine)
        {
            return new proto.Engine
            {
                engineID = engine.engineID,
                engineName = engine.engineName,
                redisIpAddress = engine.redisIpAddress,
                redisIpPort = engine.redisIpPort,
                redisDB = (ulong)engine.redisDB,
                FIXEngineIpAddress = engine.FIXEngineIpAddress,
                FIXEngineIpPort = engine.FIXEngineIpPort.ToString()
            };
        }

    }
}
