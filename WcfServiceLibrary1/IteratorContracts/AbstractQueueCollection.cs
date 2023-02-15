using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.IteratorContracts
{
    public interface AbstractQueueCollection<T>
    {
        Iterator.Iterator<T> CreateIterator();
    }
}
