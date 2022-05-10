using FIXMonitorBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace FIXMonitorBusinessLogicLayer
{
    public class Observable : IObservable<Object>
    {
        readonly static Dictionary<string, IObserver<Object>> observers = new Dictionary<string, IObserver<object>>();
        private string _connectionId;
        static ReaderWriterLock rwl = new ReaderWriterLock();
        public IDisposable Subscribe(IObserver<Object> observer)
        {
            return new Unsubscriber(observers, _connectionId);
        }

        public IDisposable Subscribe(IObserver<Object> observer, string connectionId)
        {
            rwl.AcquireWriterLock(1000);
            if (observers.ContainsKey(connectionId))
            {
                observers.Remove(connectionId);
            }
            Console.WriteLine("[Service Client] Client connected: " + connectionId);
            observers.Add(connectionId, observer);
            _connectionId = connectionId;
            rwl.ReleaseWriterLock();
            return Subscribe(observer);
        }

        public bool IsSubscribed(string connectionId)
        {
            if (observers.ContainsKey(connectionId))
            {
                return true;
            }
            return false;
        }

        //public void UpdateOrders(Object orderDetails, string operation)
        //{
        //    foreach (var item in observers)
        //    {
        //        item.Value.OnNext(new Object[] { orderDetails, operation });
        //    }
        //}

        //public void UpdateExecutions(Object executionReport, string operation)
        //{
        //    foreach (var item in observers)
        //    {
        //        item.Value.OnNext(new Object[] { executionReport, operation });
        //    }
        //}

        //public void UpdateBook(Object bookUpdates, string bookType)
        //{
        //    foreach (var item in observers)
        //    {
        //        item.Value.OnNext(new Object[] { bookUpdates, bookType });
        //    }
        //}

        //public void ClearBook(string bookType)
        //{
        //    foreach (var item in observers)
        //    {
        //        item.Value.OnNext(new Object[] { "clearbook", bookType });
        //    }
        //}

        public void SendFixMessageUpdate(Object fixMessage, string engineID, string sessionID)
        {
            foreach (var item in observers)
            {
                SendFixMessageUpdate:
                if (!rwl.IsWriterLockHeld)
                {
                    item.Value.OnNext(new Object[] { fixMessage, engineID, sessionID });
                }
                else
                {
                    goto SendFixMessageUpdate;
                }
            }
        }

        public void SendFixSessionUpdate(Object fixSession, string engineID, string updateType)
        {
            SendFixSessionUpdate:
            if (!rwl.IsWriterLockHeld)
            {
                foreach (var item in observers)
                {
                    item.Value.OnNext(new Object[] { fixSession, engineID, updateType });
                }
            }
            else
            {
                goto SendFixSessionUpdate;
            }
        }

        public void Heartbeat()
        {
            Heartbeat:
            if (!rwl.IsWriterLockHeld)
            {
                foreach (var item in observers)
                {
                    item.Value.OnNext(new Object[] { "heartbeat", "" });
                }
            }
            else
            {
                goto Heartbeat;
            }
        }

        public void SendAlertFlag(AlertFlag flag)
        {
            foreach (var item in observers)
            {
                item.Value.OnNext(new Object[] { flag });
            }
        }
    }

    public class Unsubscriber : IDisposable
    {
        private Dictionary<string, IObserver<Object>> _observers;
        readonly private string _observerConnectionId;

        public Unsubscriber(Dictionary<string, IObserver<Object>> observers, string observerConnectionId)
        {
            this._observers = observers;
            this._observerConnectionId = observerConnectionId;
        }

        public void Dispose()
        {
            if (_observerConnectionId != null && _observers.ContainsKey(_observerConnectionId))
                _observers.Remove(_observerConnectionId);
        }
    }
}
