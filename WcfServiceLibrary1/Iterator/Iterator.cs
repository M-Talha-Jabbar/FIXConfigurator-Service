using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.Iterator
{
    public class Iterator<T> : AbstractIterator<T>
    {
        private ConcreteQueueCollection<T> collection;

        public Iterator(ConcreteQueueCollection<T> collection)
        {
            this.collection = collection;
        }

        public T First()
        {
            if (!IsCompleted)
                return collection.GetPeek();
            else
                return default(T);
        }

        public T Next()
        {
            if (!IsCompleted)
                return collection.Dequeue();
            else
                return default(T);
        }

        public bool IsCompleted
        {
            get { return collection.Count == 0; }
        }
    }
}
