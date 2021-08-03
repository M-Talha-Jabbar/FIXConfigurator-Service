using System.Collections.Generic;

namespace FIXMonitorBusinessLogicLayer.DataModels
{
    public class Account
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class AccountGroup
    {
        public string GroupId { get; set; }
        public string GroupName { get; set; }
        public List<Account> Accounts { get; set; }
        public Destination Destination { get; set; }
    }

    public class Destination
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
