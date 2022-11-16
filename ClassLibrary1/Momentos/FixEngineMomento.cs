using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Momentos
{
    public class FixEngineMomento
    {
        private FIXEngine state;
        public void SetState(FIXEngine _state)
        {
            state = _state;

        }
        public FIXEngine GetState()
        {
            return state;
        }
    }
}
