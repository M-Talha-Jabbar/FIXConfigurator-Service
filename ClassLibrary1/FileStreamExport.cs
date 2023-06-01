using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using FIXMonitorBusinessLogicLayer.LocksManager;
using CoreLogging;
using FIXMonitorBusinessLogicLayer.Utilities;

namespace FIXMonitorBusinessLogicLayer
{
    class FileStreamExport
    {
        public static Stream fsExport(string filePath, object lockObject)
        {

            if (filePath == null)
            {
                return null;
            }
          
            FileStream fs;

            lock (lockObject)
            {
                try
                {
                    //Directory.Exists()
                    fs = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                    Logging.LogMessage(LOGTYPE.Info, "fix message log file read");
                    return fs;
                }

                catch (Exception e)
                {
                    Logging.LogMessage(LOGTYPE.Error, "Cannot read fix message log file.");
                    ExceptionLoggingUtility.LogException(e);
                    return null;
                }
            }

            }
        }
    }