using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.ICloneable;
using System.Collections.ObjectModel;
using System.Linq;

namespace FIXMonitorBusinessLogicLayer.KeyedCollections
{
    public class FixEnginesKeyedCollection : KeyedCollection<string, FIXEngine>, ICloneable<FixEnginesKeyedCollection>
    {
        protected override string GetKeyForItem(FIXEngine item)
        {
            return item.engineID;
        }

        public FixEnginesKeyedCollection GetClone()
        {
            FixEnginesKeyedCollection fixEnginesKeyedCollection = new FixEnginesKeyedCollection();
            foreach (var item in this.Items) fixEnginesKeyedCollection.Add(item.GetClone());

            return fixEnginesKeyedCollection;
        }
    }
}
