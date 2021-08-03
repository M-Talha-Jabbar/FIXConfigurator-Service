using FIXMonitorBusinessLogicLayer.DataModels;
using System.Collections.ObjectModel;

namespace FIXMonitorBusinessLogicLayer.KeyedCollections
{
    public class FixEnginesKeyedCollection : KeyedCollection<string, FIXEngine>
    {
        protected override string GetKeyForItem(FIXEngine item)
        {
            return item.engineID;
        }
    }
}
