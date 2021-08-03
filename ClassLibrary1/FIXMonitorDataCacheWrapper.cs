using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer
{
    public class FIXMonitorDataCacheWrapper
    {
        readonly static FIXMonitorDataCacheWrapper _FIXMonitorDataCacheWrapper = new FIXMonitorDataCacheWrapper();
        readonly private FIXMonitorDataCache _FIXMonitorDataCache;
        readonly private Thread DataCacheRefreshThread;

        private int ATSAppRefreshTime = 0;
        private int ATSAppRefreshThreadSleep = 0;

        private bool bKeepRunning = true;

        public static FIXMonitorDataCacheWrapper GetInstance()
        {
            return _FIXMonitorDataCacheWrapper;
        }

        public FIXMonitorDataCache GetATSDataCache()
        {
            return _FIXMonitorDataCache;
        }

        ~FIXMonitorDataCacheWrapper()
        {
            bKeepRunning = false;
            //CoreLogging.Logging.StopProcessing();
            DataCacheRefreshThread.Join();
            DataCacheRefreshThread.Abort();
        }

        private FIXMonitorDataCacheWrapper()
        {
            //CoreLogging.Logging.StartProcessing();
            _FIXMonitorDataCache = new FIXMonitorDataCache();

            //DataCacheRefreshThread = new Thread(new ThreadStart(RefreshCacheData));
            //DataCacheRefreshThread.Start();
        }

        private void RefreshCacheData()
        {
            bool isRefreshedForToday = false;
            while (bKeepRunning == true)
            {
                var TimeofDay = DateTime.Now.TimeOfDay;
                if (TimeofDay.Hours >= ATSAppRefreshTime && !isRefreshedForToday)
                {
                    //_FIXMonitorDataCache = null;
                    //GC.Collect();
                    //GC.WaitForPendingFinalizers();
                    //Thread.Sleep(1000);
                    //_FIXMonitorDataCache = new FIXMonitorDataCache();
                    isRefreshedForToday = true;
                }
                else if (TimeofDay.Hours < ATSAppRefreshTime)
                {
                    isRefreshedForToday = false;
                }

                Thread.Sleep(ATSAppRefreshThreadSleep);
            }
        }
    }
}
