using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GEmail
{
    public class EmailData
    {
        public string Subject { get; set; }
        public string Body { get; set; }
        public string CommaSeperatedToEmails { get; set; }
        public string CommaSeperatedCCEmails { get; set; }
    }
}
