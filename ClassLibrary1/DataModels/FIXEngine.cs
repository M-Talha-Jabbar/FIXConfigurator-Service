using FIXMonitorBusinessLogicLayer.KeyedCollections;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXEngine
    {
        public string engineID { get; set; } = Guid.NewGuid().ToString();
        public string engineName { get; set; }
        public string ipAddress { get; set; }
        public string redisIpAddress { get; set; }
        public string redisIpPort { get; set; }
        public int port { get; set; }
        public FixSessionKeyedCollection fixSessions { get; set; } = new FixSessionKeyedCollection();

        public static HashEntry[] getHashFromObject(FIXEngine obj)
        {
            HashEntry[] engineHashEntries = new HashEntry[6];
            int i = 0;
            Console.WriteLine("===========================");
            foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(obj))
            {
                string name = descriptor.Name;
                string value = JsonConvert.SerializeObject(descriptor.GetValue(obj));
                Console.WriteLine("{0}={1}", name, value);
                engineHashEntries[i++] = new HashEntry(name, new RedisValue(value));
                if (i == 6) break;
            }
            Console.WriteLine("===========================");
            return engineHashEntries;

        }

        public static void setObjectFromHash(FIXEngine obj, HashEntry[] engineHashEntries)
        {
            int i = 0;
            Console.WriteLine("===========================");
            foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(obj))
            {
                string name = descriptor.Name;

                if (name == engineHashEntries[i].Name)
                {
                    int port;
                    if (Int32.TryParse(engineHashEntries[i].Value.ToString(), out port))
                    {
                        descriptor.SetValue(obj, port);
                    }
                    else
                        descriptor.SetValue(obj, engineHashEntries[i].Value.ToString().Trim('"'));
                }
                i++;
                if (i == 6) break;
            }
            Console.WriteLine("Engine Name : " + obj.engineName);
            Console.WriteLine("===========================");

        }
    }
}
