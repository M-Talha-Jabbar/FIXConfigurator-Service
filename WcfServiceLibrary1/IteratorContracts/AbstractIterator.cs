using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorService.Iterator
{
    public interface AbstractIterator<T>
    {
        T First(); 
        T Next();
        bool IsCompleted { get; } 
    }
}
