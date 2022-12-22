using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FIXMonitorBusinessLogicLayer.ICloneable
{
    public interface ICloneable<T>
    {
        T GetClone();
    }
}
