using FIXMonitorService.Iterator;
using FIXMonitorService.PayLoads;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.QueueManager
{
    public static class ConcreteQueueCollectionsManager
    {
        //public static ArrayList concreteQueueCollections = new ArrayList();
        public static ConcurrentBag<object> concreteQueueCollections = new ConcurrentBag<object>();

        public static ConcreteQueueCollection<T> CreateOrGetConcreteQueueCollection<T>()
        {
            var Queue = concreteQueueCollections.OfType<ConcreteQueueCollection<T>>();

            if (Queue.Any())
            {
                return Queue.First();
            }

            var newQueue = new ConcreteQueueCollection<T>();
            concreteQueueCollections.Add(newQueue);
            return newQueue;
        }

        public static void SendQueuedUpdates<T>(IFIXMonitorServiceCallback callback)
        {
            var Queue = CreateOrGetConcreteQueueCollection<T>();
            //Console.WriteLine($"Queue count of {typeof(T).FullName} is : {Queue.Count}");
            var iterator = Queue.CreateIterator();

            while (((IChannel)callback).State == CommunicationState.Opened && Queue.Count > 0 && !iterator.IsCompleted)
            {
                var item = (IUpdate)iterator.Next();
                item.SendUpdateToClient(callback);
            }
            //Console.WriteLine($"Queue count of {typeof(T).FullName} is : {Queue.Count}");
        }
    }
}
