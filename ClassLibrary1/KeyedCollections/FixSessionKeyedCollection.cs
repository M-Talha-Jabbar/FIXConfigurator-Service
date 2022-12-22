using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.ICloneable;
using System.Collections.ObjectModel;

namespace FIXMonitorBusinessLogicLayer.KeyedCollections
{
    public class FixSessionKeyedCollection : KeyedCollection<string, FIXSession>, ICloneable<FixSessionKeyedCollection>
    {
        protected override string GetKeyForItem(FIXSession item)
        {
            return item.SenderCompID+"-"+item.TargetCompID;
        }

        public FixSessionKeyedCollection GetClone()
        {

            FixSessionKeyedCollection fixSessionKeyedColection = new FixSessionKeyedCollection();
            foreach(var item in this.Items) fixSessionKeyedColection.Add(item.GetClone());

            return fixSessionKeyedColection;
        }
    }
}
