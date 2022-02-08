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
        private FixEnginesKeyedCollection state;
        public void SetState(FixEnginesKeyedCollection _state)
        {
            state = _state;

        }
        public FixEnginesKeyedCollection GetState()
        {
            return state;
        }
    }
}
