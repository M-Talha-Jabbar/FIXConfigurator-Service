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
                var item = ((Object[])value)[0];

                if (item.GetType() == typeof(string) && item.ToString() == "heartbeat")
                {
                    FIXMonitorService.GetInstance().Heartbeat();
                }
                else if(item.GetType() == typeof(FIXMessage))
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
            }
            catch (Exception ex)
            {

            }
        }
    }
}
