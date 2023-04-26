using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace FIXMonitorBusinessLogicLayer.ResponseDataModels
{
    [DataContract]
    public class JenkinsJobStatus
    {
        [DataMember]
        public bool inProgress;
        [DataMember]
        public string result;
        [DataMember]
        public int id;
    }
}
