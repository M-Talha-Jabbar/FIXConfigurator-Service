using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;

namespace FIXMonitorBusinessLogicLayer
{
    class FileStreamExport
    {

        public static Stream fsExport(string filePath) {

            object lockObject = FixMessageLog.GetLockObject(filePath);

            lock (lockObject)
            {
                try
            {
                    using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read)) {
                        Console.WriteLine("file read");
                        return fs;
                    }
                        
                }
                catch (Exception ex) {

                    Console.WriteLine("file reading lock error: ", ex.Message);
                    return null;
                }

            }
        }
    }
}
