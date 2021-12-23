using FIXMonitorBusinessLogicLayer.Handler;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class FIXMessage
    {
        const char FIX_MESSAGE_DELIMITER = '|';

        public string fixMessage { get; set; }
        public string messageType { get; set; }
        public string sendingTime { get; set; }
        public List<Tuple<string, string, string>> keyValuePair { get; set; }

        public static implicit operator FIXMessage(proto.Body body)
        {
            return new FIXMessage
            {
                fixMessage = body.FIXMessage,
                messageType = body.MessageType,
                sendingTime = body.SendingTime,
                keyValuePair = ParseAndStoreFixMessage(body.FIXMessage)
            };
        }

        public static List<Tuple<string, string, string>> ParseAndStoreFixMessage(string fixMessage)
        {
            List<Tuple<string, string, string>> listOfKeyValuePair = new List<Tuple<string, string, string>>();
            string[] keyValuePairs = fixMessage.Split(new char[] { FIX_MESSAGE_DELIMITER, '\u0001' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < keyValuePairs.Length; i++)
            {
                string[] keyValuePair = keyValuePairs[i].Trim().Split('=');
                Tuple<string, string, string> tuple;
                if (FixHandler.fixTagValues.ContainsKey(keyValuePair[0]))
                {
                    tuple = Tuple.Create(keyValuePair[0], FixHandler.fixTagValues[keyValuePair[0]], keyValuePair[1]);
                }
                else
                {
                    tuple = Tuple.Create(keyValuePair[0], keyValuePair[0], keyValuePair[1]);
                }
                listOfKeyValuePair.Add(tuple);
            }
            return listOfKeyValuePair;
        }

        public static string GetFixTagValue(string fixMessage, string tag)
        {
            string[] keyValuePairs = fixMessage.Split(new char[] { FIX_MESSAGE_DELIMITER, '\u0001' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < keyValuePairs.Length; i++)
            {
                string[] keyValuePair = keyValuePairs[i].Trim().Split('=');
                if (keyValuePair[0].Trim() == tag.Trim())
                {
                    return keyValuePair[1];
                }
            }
            return "";
        }
    }

    
}
