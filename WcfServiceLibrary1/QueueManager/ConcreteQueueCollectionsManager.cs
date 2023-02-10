using FIXMonitorService.Iterator;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.QueueManager
{
    public static class ConcreteQueueCollectionsManager
    {
        public static ArrayList concreteQueueCollections = new ArrayList();

        public static ConcreteQueueCollection<T> CreateOrGetConcreteQueueCollection<T>()
        {
            var Queue = concreteQueueCollections.OfType<ConcreteQueueCollection<T>>();

            if(Queue.Any())
            {
                return Queue.First();
            }

            var newQueue = new ConcreteQueueCollection<T>();
            concreteQueueCollections.Add(newQueue);
            return newQueue;
        }
    }
}
