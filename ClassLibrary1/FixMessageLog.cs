using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer
{
    class FixMessageLog
    {
        // creating files for each session.
        // whenever new message arrive append into the file
        // parsing fix messages and replacing seperator with pipe, appending either msg is IN or OUT, Date of the logging
        // Sending Stream to web client
        // "./FixMessagesLogs/test.txt"

        // key is engineName+sessionid and values is filename. whenever will be file created we append to this dictionary

        private static ConcurrentDictionary<string, string> CreatedFiles = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<string, object> s_fileLocks = new ConcurrentDictionary<string, object>();
        private static string FixMessageLogDirectoryPath = ConfigurationManager.AppSettings["FixMessageLogDirectoryPath"];


        public static bool IsFileCreated(string sessionId, string engineName)
        {
            return CreatedFiles.ContainsKey($"{engineName}-{sessionId}") ? true : false;
        }

        public static object GetLockObj(string filePath) {
            object lockForThisFile;
            if (s_fileLocks.ContainsKey(filePath))
            {
                lockForThisFile = s_fileLocks[filePath];
            }
            else
            {
                lockForThisFile = new object();
                s_fileLocks.TryAdd(filePath, lockForThisFile);
            }

            return lockForThisFile;
        }

        public static void AddFixMessageLog(string fixMessageLog, string filePath) /// ////
        {
            lock (GetLockObj(filePath))
            {
                using (StreamWriter sw = File.AppendText(filePath))
                {
                    sw.WriteLine(fixMessageLog);
                    sw.Flush();
                }
            }
        }

        public static string FixMessageLogFormatter(FIXMessage fixMessage, FIXSession fixSession)
        {
            // DateTime 
            // Finding Message is comming or going in/from fixhub
            // fixmessage replacing seperator to pipe

            string messageSender;

            if (fixSession.SenderCompID == fixMessage.keyValuePair[4].Item3)
            {
                messageSender = "OUT";
            }
            else
            {
                messageSender = "IN";
            }

            string formattedDate = DateTime.Now.ToString("yyyyMMdd-HH:mm:ss.fff");

            string formattedFixMessage = fixMessage.fixMessage.Replace("\u0001", " | ").Insert(0, $" | {messageSender} | ").Insert(0, formattedDate);

            return formattedFixMessage;
        }

        public static string LogFileNameCreator(string sessionId, string engineName)
        {
            var logFileName = $"{engineName}-{sessionId}-{DateTime.Now.ToString("yyyyMMdd")}";
            return logFileName;
        }

        public static string LogFilePathCreator(string logFileName)
        {
            var logFilePath = $"{FixMessageLogDirectoryPath}{logFileName}.txt";
            return logFilePath;
        }

        public static void FixMessageLogger(string sessionId, FIXEngine fixEngine, FIXMessage fixMessage)
        {
            string logFilePath;
            string createdFilekey = $"{fixEngine.engineName}-{sessionId}";
            bool isFileCreated = IsFileCreated(sessionId, fixEngine.engineName);

            var fixSession = fixEngine.fixSessions[sessionId];

            string formattedFixMessageLog = FixMessageLogFormatter(fixMessage, fixSession);

            if (!isFileCreated)
            {
                logFilePath = LogFilePathCreator(LogFileNameCreator(sessionId, fixEngine.engineName));
                CreatedFiles.TryAdd(createdFilekey, logFilePath);
            }
            else
            {
                logFilePath = CreatedFiles[createdFilekey];
            }

            AddFixMessageLog(formattedFixMessageLog, logFilePath);
        }

        public static string GetFixMessageLogFilePath(string sessionId, string engineName)
        {
            string filePath;
            CreatedFiles.TryGetValue($"{engineName}-{sessionId}", out filePath);
            if (filePath == null) { 
                filePath = LogFilePathCreator(LogFileNameCreator(sessionId, engineName));
                if (File.Exists(filePath)) CreatedFiles.TryAdd($"{engineName}-{sessionId}", filePath);
            }
            return filePath;
        }

    }
}
