using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Utilities
{
    static class TimeStampUtility
    {
        public static long ConvertTimeStampToLong(string timeStamp)
        {
            return long.Parse(timeStamp);
        }

        public static string TimeStampExcludingSequenceNumber(string timeStamp)
        {
            return timeStamp.Split('-')[0];
        }

        public static bool CompareTimeStamps(string currentlastReadTimeStamp, string lastReadTimeStampInRedis)
        {
            long currentlastReadTimeStampInLong = ConvertTimeStampToLong(TimeStampExcludingSequenceNumber(currentlastReadTimeStamp));
            long lastReadTimeStampInRedisInLong = ConvertTimeStampToLong(TimeStampExcludingSequenceNumber(lastReadTimeStampInRedis));

            if (currentlastReadTimeStampInLong < lastReadTimeStampInRedisInLong)
                return true;

            return false;
        }
    }
}
