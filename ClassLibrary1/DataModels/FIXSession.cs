using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXSession
    {
        public string ConnectionID { get; set; }

        public string Status { get; set; } = "disconnected";

        public string SenderCompID { get; set; }

        public string TargetCompID { get; set; }

        public string IPAddress { get; set; }

        public string BackUpIPAddress { get; set; }

        public int Port { get; set; }

        public int BackUpPort { get; set; }

        public bool Validate { get; set; }

        public bool HandleResend { get; set; }

        public int HeartBeartInterval { get; set; }

        public int MaxLatency { get; set; }

        public bool ResetConnection { get; set; }

        public bool EnableConnection { get; set; }

        public string FIXVersion { get; set; }

        public string InternalFIXVersion { get; set; }

        public string Mode { get; set; }

        public bool DBEnabled { get; set; }

        public bool LatencyEnabled { get; set; }

        public bool AutoConnect { get; set; }

        public bool AutoReconnect { get; set; }

        public int ReconnectDelay { get; set; }

        public int ConnectRetry { get; set; }

        public string LogonRawData { get; set; }

        public bool MilliSecondTime { get; set; }

        public bool? QEnabled { get; set; }

        public DateTime SessionStart { get; set; }

        public DateTime SessionEnd { get; set; }

        public string TaskReset { get; set; }

        public int InSecNum { get; set; }

        public int OutSecNum { get; set; }

        public DateTime LastUpdated { get; set; }

        public List<FIXMessage> FixMessages { get; set; } = new List<FIXMessage>();


        public static HashEntry[] getHashFromObject(FIXSession obj)
        {
            var properties = TypeDescriptor.GetProperties(obj);
            HashEntry[] engineHashEntries = new HashEntry[properties.Count - 1];
            int i = 0;
            Console.WriteLine("===========================");
            foreach (PropertyDescriptor descriptor in properties)
            {
                string name = descriptor.Name.ToUpper();
                string value = JsonConvert.SerializeObject(descriptor.GetValue(obj));
                Console.WriteLine("{0}={1}", name, value);
                engineHashEntries[i++] = new HashEntry(name, new RedisValue(value));
                if (i == properties.Count - 1) break;
            }
            Console.WriteLine("===========================");
            return engineHashEntries;

        }

        public static void setObjectFromHash(FIXSession obj, HashEntry[] engineHashEntries)
        {
            Dictionary<string, int> indexes = new Dictionary<string, int>();
            for (int k = 0; k < engineHashEntries.Length; k++)
            {
                indexes.Add(engineHashEntries[k].Name.ToString().ToUpper(), k);
            }

            int i = 0;
            Console.WriteLine("===========================");
            foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(obj))
            {
                string name = descriptor.Name;

                if (!indexes.ContainsKey(name.ToUpper())) continue;

                i = indexes[name.ToUpper()];
                if (name.ToUpper() == engineHashEntries[i].Name.ToString().ToUpper())
                {
                    int numericItem;
                    bool boolItem;
                    DateTime dtItem;
                    var val = engineHashEntries[i].Value.ToString();
                    if (Int32.TryParse(val, out numericItem))
                    {
                        descriptor.SetValue(obj, numericItem);
                    }
                    else if (Boolean.TryParse(val, out boolItem))
                    {
                        descriptor.SetValue(obj, boolItem);
                    }
                    else if (DateTime.TryParse(val, out dtItem))
                    {
                        descriptor.SetValue(obj, dtItem);
                    }
                    else
                    {
                        var trimValue = engineHashEntries[i].Value.ToString().Trim('"');
                        var arr = trimValue.Split('\0');
                        descriptor.SetValue(obj,arr[arr.Length-1]);
                    }
                }
                i++;
                if (i == 4) break;
            }
            Console.WriteLine("===========================");
            if (!indexes.ContainsKey("ipaddress".ToUpper()))
            {
                obj.IPAddress = "127.0.0.1";
            }

        }

    }
}
