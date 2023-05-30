using CoreLogging;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace FIXMonitorBusinessLogicLayer
{
    public class Observable : IObservable<Object>
    {
        readonly static ConcurrentDictionary<string, IObserver<Object>> observers = new ConcurrentDictionary<string, IObserver<object>>();
        private string _connectionId;
        public IDisposable Subscribe(IObserver<Object> observer)
        {
            return new Unsubscriber(observers, _connectionId);
        }

        public IDisposable Subscribe(IObserver<Object> observer, string connectionId)
        {
            Logging.LogMessage(LOGTYPE.Info, "[Observer] Client connected: " + connectionId);

            observers.TryAdd(connectionId, observer);
            _connectionId = connectionId;

            return Subscribe(observer);
        }

        public static bool IsSubscribed(string connectionId)
        {
            if (observers.ContainsKey(connectionId))
            {
                return true;
            }

            return false;
        }

        public void SendFixMessageUpdate(Object fixMessage, string engineID, string sessionID)
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { fixMessage, engineID, sessionID });
            }
        }

        public void SendFixSessionUpdate(Object fixSession, string engineID, string updateType)
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { fixSession, engineID, updateType });
            }
        }

        public void SendFixSessionStatusMessage(string fixSessionStatusMessage, string updateType)
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] {fixSessionStatusMessage, updateType });
            }
        }


        public void SendFixMessageContainingConfiguredFixTagValuePairUpdate(Object fixMessage, string engineID, string sessionID)
        {
            foreach(var item in observers)
            {
                item.Value.OnNext(new Object[] { "fixMessageWithConfiguredFixTagValuePair", fixMessage, engineID, sessionID });
            }
        }

        public void Heartbeat()
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { "heartbeat", "" });
            }
        }

        public void SendAlertFlag(AlertFlag flag)
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { flag });
            }
        }

        public void SendJenkinsJobStatus(JenkinsJobStatus jenkinsJobStatus) 
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { jenkinsJobStatus });
            }
        }
    }

    public class Unsubscriber : IDisposable
    {
        private ConcurrentDictionary<string, IObserver<Object>> _observers;
        readonly private string _observerConnectionId;

        public Unsubscriber(ConcurrentDictionary<string, IObserver<Object>> observers, string observerConnectionId)
        {
            this._observers = observers;
            this._observerConnectionId = observerConnectionId;
        }

        public void Dispose()
        {
            if (_observerConnectionId != null && _observers.ContainsKey(_observerConnectionId))
            {
                _observers.TryRemove(_observerConnectionId, out IObserver<object> value);
            }
        }
    }
}
