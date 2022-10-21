using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.Converter
{
    static class TimeConverter
    {
        public static int GetTimeInMilliseconds(DateTime Timeout)
        {
            int hours = Timeout.Hour * 60 * 60 * 1000;
            int minutes = Timeout.Minute * 60 * 1000;
            int seconds = Timeout.Second * 1000;

            int totalMilliseconds = hours + minutes + seconds;

            return totalMilliseconds;
        }
    }
}
