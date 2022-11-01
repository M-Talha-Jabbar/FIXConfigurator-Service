using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer
{
    class FixMessageLog
    {

        // creating files for each session.
        // maintaining a dictionary with key -> sessionid & value -> list or set of files
        // whenever new message arrive append into the file
        // parsing fix messages and replacing seperator with pipe, appending either msg is IN or OUT, Date of the logging
        // Sending txt file blob to be consumed
        // "./FixMessagesLogs/test.txt"

        // key is sessionid and values is filename. whenever will be file created we append to this dictionary

        Dictionary<string, string> CreatedFiles;

        public FixMessageLog() {

            CreatedFiles = new Dictionary<string, string>();
        }

        public bool IsFileCreated(string sessionId)
        {
            return CreatedFiles.ContainsKey(sessionId) ? true : false; 
        }

        public void AddFixMessageLog(string sessionId, string fixMessageLog, string filePath) {

            CreatedFiles.Add(sessionId, filePath);

            using (StreamWriter sw = File.AppendText(filePath))
            {
                sw.WriteLine(fixMessageLog);
            }
        }

        public void AddFixMessageLog(string sessionId, string fixMessageLog)
        {
            using (StreamWriter sw = File.AppendText(CreatedFiles[sessionId]))
            {
                sw.WriteLine(fixMessageLog);
            }
        }

        public string FixMessageLogFormatter(FIXMessage fixMessage, FIXSession fixSession) 
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

        public string LogFileNameCreator(string sessionId) {

            var logFileName = $"{sessionId}-{DateTime.Now.ToString("yyyyMMdd")}";

            return logFileName;
        }

        public string LogFilePathCreator(string logFileName) {

            var logFilePath = $"./FixMessagesLogs/{logFileName}.txt";

            return logFilePath;
        }

        public void FixMessageLogger(string sessionId, FIXSession fixSession, FIXMessage fixMessage)
        {

            bool isFileCreated = IsFileCreated(sessionId);

            string formattedFixMessageLog = FixMessageLogFormatter(fixMessage, fixSession);

            if (isFileCreated)
            {
                AddFixMessageLog(sessionId, formattedFixMessageLog);
            }
            else {

                var logFilePath = LogFilePathCreator(LogFileNameCreator(sessionId));

                AddFixMessageLog(sessionId, formattedFixMessageLog, logFilePath);
            }
        }

        public void test() {

            //if (!File.Exists(path))
            //{
            //    // Create a file to write to.
            //    using (StreamWriter sw = File.CreateText(path))
            //    {
            //        sw.WriteLine("Hello");
            //        sw.WriteLine("And");
            //        sw.WriteLine("Welcome");
            //    }
            //}

            try
            {
               
                using (StreamWriter sw = File.AppendText("./FixMessagesLogs/test.txt"))
                {
                    sw.WriteLine("This");
                    sw.WriteLine("is Extra");
                    sw.WriteLine("Text");
                }
            }
            catch (Exception ex) { 
                
            }
           
        }


    }
}
