using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.LocksManager
{ 
    class LockObjectsManager
    {
        private readonly ConcurrentDictionary<string, object> LockObj = new ConcurrentDictionary<string, object>();

        public object GetLockObj(string filePath)
        {
            object lockForThisFile;
            if (LockObj.ContainsKey(filePath))
            {
                lockForThisFile = LockObj[filePath];
            }
            else
            {
                lockForThisFile = new object();
                LockObj.TryAdd(filePath, lockForThisFile);
            }
            
            return lockForThisFile;
        }
    }
}
