using System;
using System.Collections.Generic;

// Code scaffolded by EF Core assumes nullable reference types (NRTs) are not used or disabled.
// If you have enabled NRTs for your project, then un-comment the following line:
// #nullable disable

namespace FIXMonitorBusinessLogicLayer.Data
{
    public partial class FixEngines
    {
        public string EngineId { get; set; }
        public string EngineName { get; set; }
        public string RedisServer { get; set; }
        public string RedisPort { get; set; }
        public int RedisDb { get; set; }
    }
}
