using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;

namespace FIXMonitorService
{
    public class OrderObserver : IObserver<Object>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(Object value)
        {
            try
            {
                var updateItem = ((Object[])value);

                var item = ((Object[])value)[0];

                if (item.GetType() == typeof(string) && item.ToString() == "heartbeat")
                {
                    FIXMonitorService.GetInstance().Heartbeat();
                }

                else if (((Object[])value)[1].GetType() == typeof(FIXMessage) && item.ToString() == "fixMessageWithConfiguredFixTagValuePair")
                {
                    var fixMessage = ((Object[])value)[1];
                    var engineID = ((Object[])value)[2].ToString();
                    var sessionID = ((Object[])value)[3].ToString();
                    FIXMonitorService.GetInstance().SendFixMessageWithConfiguredFixTagValuePairToClient((FIXMessage)fixMessage, engineID, sessionID);
                }

                else if (item.GetType() == typeof(FIXMessage))
                {
                    var engineID = ((Object[])value)[1].ToString();
                    var sessionID = ((Object[])value)[2].ToString();
                    FIXMonitorService.GetInstance().SendFixMessagesToClient((FIXMessage)item, engineID, sessionID);
                }

                else if (item.GetType() == typeof(FIXSession))
                {
                    var engineID = ((Object[])value)[1].ToString();
                    var commandType = ((Object[])value)[2].ToString();
                    FIXMonitorService.GetInstance().SendFixSessionToClient((FIXSession)item, engineID, commandType);
                }

                else if (item.GetType() == typeof(AlertFlag))
                {
                    FIXMonitorService.GetInstance().SendAlertFlag((AlertFlag)item);
                }
                else if (updateItem.Length == 2 && (string)updateItem[1] == "fixSessionStatusMessage") {

                    FIXMonitorService.GetInstance().SendFixSessionStatusMessage((string)updateItem[0]);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString(), ex.Message);
            }
        }
    }
}
