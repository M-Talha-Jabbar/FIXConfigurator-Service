using CoreLogging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Utilities
{
    public static class ExceptionLoggingUtility
    {
        public static void LogException(Exception e)
        {
            Logging.LogMessage(LOGTYPE.Error, "Exception : " + e.Message);
            Logging.LogMessage(LOGTYPE.Error, "StackTrace : " + e.StackTrace);

            if (e.InnerException != null)
            {
                Logging.LogMessage(LOGTYPE.Error, "Inner Exception : " + e.InnerException.Message);
                Logging.LogMessage(LOGTYPE.Error, "StackTrace Inner Exception : " + e.InnerException.StackTrace);
            }
        }
    }
}
