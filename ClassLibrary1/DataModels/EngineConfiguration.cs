using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class EngineConfiguration
    {
        public string EngineId { get; set; }
        public string EngineName { get; set; }
        public string RedisServer { get; set; }
        public string RedisPort { get; set; }
        public int RedisDb { get; set; }
    }
}
