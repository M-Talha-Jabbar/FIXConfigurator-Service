using ATSBusinessLogicLayer.DataModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RealTimeUpdater
{
    public class Class1
    {
    }

    public class OrderObserver : IObserver<List<OrderDetails>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(List<OrderDetails> value)
        {

        }
    }

    public class OrderObservable : IObservable<List<OrderDetails>>
    {
        List<IObserver<List<OrderDetails>>> observers = new List<IObserver<List<OrderDetails>>>();
        public IDisposable Subscribe(IObserver<List<OrderDetails>> observer)
        {
            observers.Add(observer);
            return new Unsubscriber(observers, observer);
        }

        public void UpdateOrders(List<OrderDetails> orderDetails)
        {
            foreach (var item in observers)
            {
                item.OnNext(orderDetails);
            }
        }
    }

    public class Unsubscriber : IDisposable
    {
        private List<IObserver<List<OrderDetails>>> _observers;
        private IObserver<List<OrderDetails>> _observer;

        public Unsubscriber(List<IObserver<List<OrderDetails>>> observers, IObserver<List<OrderDetails>> observer)
        {
            this._observers = observers;
            this._observer = observer;
        }

        public void Dispose()
        {
            if (_observer != null && _observers.Contains(_observer))
                _observers.Remove(_observer);
        }
    }
}
