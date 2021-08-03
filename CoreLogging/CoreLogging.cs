using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NLog.Config;
using NLog;
using NLog.Targets;
using System.Collections.Concurrent;
using System.Threading;
using System.IO;

namespace CoreLogging
{
    public enum LOGTYPE
    {
        Debug,
        Info,
        Warn,
        Error,
        Fatal,
        EF,
        TraceGrpcMessages
    }

    public class LogMsg
    {
        public LOGTYPE LogType;
        public string stMessage;
    }

    public class Logging
    {
        private static Logger logger;
        private static ConcurrentQueue<LogMsg> LoggingQueue = new ConcurrentQueue<LogMsg>();
        private static bool bKeepRunning = true;
        private static bool _bQueuebased = false;

        private static string stEF_Log_File_Name = System.DateTime.Now.ToString("yyyy-MM-dd") + " Logfile_EF.txt";
        private static StreamWriter EF_Streamwriter = new StreamWriter(stEF_Log_File_Name);

        //public Logging()
        //{
        //    InitNLogLogging();
        //    StartLogProcessing();
        //}

        public static void StartProcessing(bool bQueuebased = false)
        {
            InitNLogLogging();
            _bQueuebased = bQueuebased;
            if ( bQueuebased == true)
                StartLogProcessing();
        }

        public static void StopProcessing()
        {
            bKeepRunning = false;
        }

        private static void StartLogProcessing()
        {
            Thread LogginThread = new Thread(new ThreadStart(ProcessLoggingMsgQueue));
            LogginThread.Start();
        }

        private static void ProcessLoggingMsgQueue()
        {
            Thread.Sleep(9000);

            while (bKeepRunning == true)
            {
                LogMsg logMsg;
                if (LoggingQueue.TryDequeue(out logMsg) == false)
                {
                    //Thread.Sleep(1000);
                    Thread.Sleep(100);
                }
                else
                {
                    LogMessageInternal(logMsg);
                }
            }

            LogMsg logMsga;
            while (LoggingQueue.TryDequeue(out logMsga) == true)
            {
                LogMessageInternal(logMsga);
            }

            LogManager.Flush();
            LogManager.Shutdown();

            EF_Streamwriter.Flush();
            EF_Streamwriter.Close();
        }

        //public void InitLogging()
        //{
        //    string stDateTime = System.DateTime.Now.ToString("ddd, MMM dd, yyyy");
        //    Trace.Listeners.Add(new TextWriterTraceListener("CECAlgorithm" + stDateTime + ".log", "myListener"));
        //    Trace.AutoFlush = true;
        //    Trace.WriteLine(System.DateTime.Now.ToString("MM/dd/yy HH:mm:ss.ffff -> ") + " *************** CEC Calc Log Initialized **************** ");
        //}

        private static void LogMessageInternal(LogMsg logMsg)
        {
            switch (logMsg.LogType)
            {
                case LOGTYPE.Error:
                    {
                        LogError(logMsg.stMessage);
                        break;
                    }

                case LOGTYPE.Debug:
                    {
                        LogDebugMessage(logMsg.stMessage);
                        break;
                    }

                case LOGTYPE.Fatal:
                    {
                        LogFatalMessage(logMsg.stMessage);
                        break;
                    }

                case LOGTYPE.Info:
                    {
                        LogInfoMessage(logMsg.stMessage);
                        break;
                    }

                case LOGTYPE.Warn:
                    {
                        LogDebugMessage(logMsg.stMessage);
                        break;
                    }

                case LOGTYPE.EF:
                    {
                        LogEFMessage(logMsg.stMessage);
                        break;
                    }
                case LOGTYPE.TraceGrpcMessages:
                    {
                        LogGrpcTraceMessage(logMsg.stMessage);
                        break;
                    }
            }
        }

        private static void LogEFMessage(string stMessage)
        {
            EF_Streamwriter.WriteLine(stMessage);
        }

        private static void LogMessage_EF(string stMessage)
        {
            LogMsg logMsg = new LogMsg();
            logMsg.LogType = LOGTYPE.EF;
            logMsg.stMessage = stMessage;
            LoggingQueue.Enqueue(logMsg);
        }

        public static void LogMessage(LOGTYPE LogType, string stMessage)
        {
            LogMsg logMsg = new LogMsg();
            logMsg.LogType = LogType;
            logMsg.stMessage = stMessage;
            if (_bQueuebased)
                LoggingQueue.Enqueue(logMsg);
            else
                LogMessageInternal(logMsg);
        }

        public static void LogMessage(string stMessage)
        {
            LogMsg logMsg = new LogMsg();
            logMsg.LogType = LOGTYPE.Info;
            logMsg.stMessage = stMessage;
            if (_bQueuebased)
                LoggingQueue.Enqueue(logMsg);
            else
                LogMessageInternal(logMsg);
        }

        private static void LogError(string stMessage)
        {
            logger.Error(stMessage);
        }

        private static void LogDebugMessage(string stMessage)
        {
            logger.Debug(stMessage);
        }

        private static void LogFatalMessage(string stMessage)
        {
            logger.Fatal(stMessage);
        }

        private static void LogInfoMessage(string stMessage)
        {
            logger.Info(stMessage);
        }

        private static void LogWarnMessage(string stMessage)
        {
            logger.Warn(stMessage);
        }

        private static void LogGrpcTraceMessage(string stMessage)
        {
            logger.Trace(stMessage);
        }

        private static void InitNLogLogging()
        {
            // Step 1. Create configuration object 
            var config = new LoggingConfiguration();

            // Step 2. Create targets
            var consoleTarget = new ColoredConsoleTarget("target1")
            {
                Layout = @"${date:format=HH\:mm\:ss} ${level} ${message} ${exception}"
            };
            config.AddTarget(consoleTarget);

            var fileTarget = new FileTarget("target2")
            {
                FileName = "${basedir}/Logs/${shortdate} Logfile_verbose.txt",
                Layout = "${longdate} ${level} ${message}  ${exception}"
            };
            config.AddTarget(fileTarget);

            var fileTarget3 = new FileTarget("target3")
            {
                FileName = "${basedir}/Logs/${shortdate} Logfile_Error.txt",
                Layout = "${longdate} ${level} ${message}  ${exception}"
            };
            config.AddTarget(fileTarget3);

            var fileTarget4 = new FileTarget("target4")
            {
                FileName = "${basedir}/Logs/${shortdate} Logfile_Fatal.txt",
                Layout = "${longdate} ${level} ${message}  ${exception}"
            };
            config.AddTarget(fileTarget4);

            var fileTarget5 = new FileTarget("target5")
            {
                FileName = "${basedir}/Logs/${shortdate} GrpcMessages.txt",
                Layout = "${longdate} ${level} ${message}  ${exception}"
            };
            config.AddTarget(fileTarget5);

            // Step 3. Define rules
            //config.AddRuleForOneLevel(LogLevel.AllLevels, fileTarget); // all errors to file
            config.AddRuleForAllLevels(consoleTarget); // all to console
            config.AddRuleForAllLevels(fileTarget); // all to console
            config.AddRuleForOneLevel(LogLevel.Error, fileTarget3);
            config.AddRuleForOneLevel(LogLevel.Fatal, fileTarget4);
            config.AddRuleForOneLevel(LogLevel.Trace, fileTarget5);
            // Step 4. Activate the configuration
            LogManager.Configuration = config;

            // Example usage
            //Logger logger = LogManager.GetLogger("Example");
            logger = LogManager.GetLogger("Example");
            logger.Trace("");
            logger.Trace("");
            logger.Trace("********///// Start Execution " + System.DateTime.Now.ToShortDateString() + " " + System.DateTime.Now.ToLongTimeString() + " ////// ***********");
            logger.Error("********///// Start Execution " + System.DateTime.Now.ToShortDateString() + " " + System.DateTime.Now.ToLongTimeString() + " ////// ***********");
            logger.Fatal("********///// Start Execution " + System.DateTime.Now.ToShortDateString() + " " + System.DateTime.Now.ToLongTimeString() + " ////// ***********");

            EF_Streamwriter.WriteLine("********///// Start Execution " + System.DateTime.Now.ToShortDateString() + " " + System.DateTime.Now.ToLongTimeString() + " ////// ***********");
            //logger.Debug("debug log message");
            //logger.Info("info log message");
            //logger.Warn("warn log message");
            //logger.Error("error log message");
            //logger.Fatal("fatal log message");

            //Example of logging exceptions

            //try
            //{

            //}
            //catch (Exception ex)
            //{
            //    logger.Error(ex, "ow noos!");
            //    throw;
            //}
        }
    }
}
