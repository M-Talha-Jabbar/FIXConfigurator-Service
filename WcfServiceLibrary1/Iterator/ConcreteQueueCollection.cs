using FIXMonitorService.IteratorContracts;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.Iterator
{
    public class ConcreteQueueCollection<T> : AbstractQueueCollection<T>
    {
        private ConcurrentQueue<T> concurrentQueue = new ConcurrentQueue<T>();

        public Iterator<T> CreateIterator()
        {
            return new Iterator<T>(this);
        }

        public int Count
        {
            get { return concurrentQueue.Count; }
        }
        public void Enqueue(T item)
        {
            concurrentQueue.Enqueue(item);
        }
        public T GetPeek()
        {
            concurrentQueue.TryPeek(out T item);
            return item;
        }
        public T Dequeue()
        {
            concurrentQueue.TryDequeue(out T item);
            return item;
        }
    }
}
