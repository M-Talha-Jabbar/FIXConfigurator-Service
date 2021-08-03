using FIXMonitorBusinessLogicLayer.DataModels;
using System.Collections.ObjectModel;

namespace FIXMonitorBusinessLogicLayer.KeyedCollections
{
    public class FixSessionKeyedCollection : KeyedCollection<string, FIXSession>
    {
        protected override string GetKeyForItem(FIXSession item)
        {
            return item.ConnectionID;
        }
    }
}
