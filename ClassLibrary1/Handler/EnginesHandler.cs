using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.IHandler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Handler
{
    class EnginesHandler : IEnginesHandler
    {
        public EnginesHandler() {}

        public async Task<bool> AddEngineConfiguration(EngineConfiguration engineConfiguration)
        {
            if (engineConfiguration != null)
            {
                var engine = new FixEngines()
                {
                    EngineId = engineConfiguration.EngineId,
                    EngineName = engineConfiguration.EngineName,
                    RedisServer = engineConfiguration.RedisServer,
                    RedisPort = engineConfiguration.RedisPort,
                    RedisDb = engineConfiguration.RedisDb
                };

                using (var context = new FIXMonitorContext())
                {
                    await context.FixEngines.AddAsync(engine);
                    await context.SaveChangesAsync();
                }

                return true;
            }

            return false;
        }

        public async Task<bool> DeleteEngineConfiguration(string EngineId)
        {
            if (!string.IsNullOrEmpty(EngineId))
            {
                using (var context = new FIXMonitorContext())
                {
                    var engine = await context.FixEngines.FirstOrDefaultAsync(s => s.EngineId == EngineId);

                    if (engine != null)
                    {
                        context.FixEngines.Remove(engine);
                        await context.SaveChangesAsync();

                        return true;
                    }

                    return false;
                }
            }

            return false;
        }

        public async Task<List<EngineConfiguration>> GetAllEnginesConfiguration()
        {
            using (var context = new FIXMonitorContext())
            {
                List<EngineConfiguration> allEnginesConfiguration = new List<EngineConfiguration>();

                var res = await context.FixEngines.ToListAsync();

                if (res.Count > 0)
                {
                    allEnginesConfiguration = res.Select(config => new EngineConfiguration()
                    {
                        EngineId = config.EngineId,
                        EngineName = config.EngineName,
                        RedisServer = config.RedisServer,
                        RedisPort = config.RedisPort,
                        RedisDb = config.RedisDb
                    }).ToList();

                    return allEnginesConfiguration;
                }

                return allEnginesConfiguration;
            }
        }

        public async Task<EngineConfiguration> GetEngineConfiguration(string EngineId)
        {
            EngineConfiguration engineConfiguration = null;

            if (!string.IsNullOrEmpty(EngineId))
            {
                using (var context = new FIXMonitorContext())
                {
                    var engineInfo = await context.FixEngines.FirstOrDefaultAsync(s => s.EngineId == EngineId);

                    if (engineInfo != null)
                    {
                        engineConfiguration = new EngineConfiguration()
                        {
                            EngineId = engineInfo.EngineId,
                            EngineName = engineInfo.EngineName,
                            RedisServer = engineInfo.RedisServer,
                            RedisPort = engineInfo.RedisPort,
                            RedisDb = engineInfo.RedisDb
                        };

                        return engineConfiguration;
                    }

                    return engineConfiguration;
                }
            }

            return engineConfiguration;
        }
    }
}
