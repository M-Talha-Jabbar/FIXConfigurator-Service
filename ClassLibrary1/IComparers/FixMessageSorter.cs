using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.Handler;
using FIXMonitorBusinessLogicLayer.IHandler;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.IComparers
{
    class FixMessageSorter : IComparer<HashEntry>
    {

        public FixMessageSorter(IFixHandler fixHandler)
        {

        }

        public int Compare(HashEntry x, HashEntry y)
        {
            var xVal =  x.Value;
            var xTime = FIXMessage.GetFixTagValue(xVal, "52");
            var yVal = y.Value;
            var yTime = FIXMessage.GetFixTagValue(yVal, "52");

            return (new CaseInsensitiveComparer()).Compare(xTime, yTime);

        }
    }
}
