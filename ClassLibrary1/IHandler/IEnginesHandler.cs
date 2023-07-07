using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    public interface IEnginesHandler
    {
        Task<EngineConfiguration> GetEngineConfiguration(string EngineId);
        Task<bool> AddEngineConfiguration(EngineConfiguration engineConfiguration);
        Task<bool> DeleteEngineConfiguration(string EngineId);
        Task<List<EngineConfiguration>> GetAllEnginesConfiguration();
    }
}
