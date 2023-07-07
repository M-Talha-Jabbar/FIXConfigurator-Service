using DevExtreme.AspNet.Data.ResponseModel;
using FIXMonitorBusinessLogicLayer.Data;
using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using FIXMonitorBusinessLogicLayer.ResponseDataModels;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Threading.Tasks;

namespace FIXMonitorService
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "IFIXMonitorService" in both code and config file together.
    [ServiceContract(CallbackContract = typeof(IFIXMonitorServiceCallback), SessionMode = SessionMode.Required)]
    public interface IFIXMonitorService
    {
        [OperationContract]
        string GetData(int value);

        [OperationContract]
        CompositeType GetDataUsingDataContract(CompositeType composite);

        [OperationContract]
        void AddFIXConfiguration(FIXConfiguration fixConfiguration);

        [OperationContract]
        IEnumerable<FIXConfiguration> GetFIXConfigurations();

        [OperationContract]
        FixSessionKeyedCollection GetFixSessions(string FixEngineID);

        [OperationContract]
        IEnumerable<string> GetFixMessageTypesFilter();

        [OperationContract]
        Dictionary<string, List<string>> GetFixTagValuePairFilter();

        [OperationContract]
        IEnumerable<FIXSessionsConnectivityStatus> GetFixSessionsConnectivityStatus();

        [OperationContract]
        void InvokeSessionUpdates(string engineName);

        [OperationContract]
        bool ConnectToFIX(FIXSession fixSession);

        [OperationContract]
        bool DisconnectToFIX(FIXSession fixSession);

        [OperationContract]
        bool ResetSequenceNumber(FIXSession fixSession);

        [OperationContract]
        bool SetSequenceNumber(FIXSession fixSession);

        [OperationContract]
        FixEnginesKeyedCollection GetFixEngines();

        [OperationContract]
        FIXEngine ConnectToFixEngine(FIXEngine fixEngine);

        [OperationContract]
        FIXEngine DisconnectToFixEngine(FIXEngine fixEngine);

        [OperationContract]
        FIXSession ConnectToFixSession(string engineID, FIXSession fixSession);

        [OperationContract(IsOneWay = true)]
        void Subscribe(string connectionId);

        [OperationContract]
        bool IsSubscribed(string connectionId);

        [OperationContract]
        string GetFixMessages(string fixEngineID, string fixSessionConnectionID, string dataSourceLoadOptions);

        [OperationContract]
        List<FIXMessage> GetFixMessagesHavingAnyConfiguredFixTagValuePair(string sessionID);

        [OperationContract]
        List<AlertFlag> GetAlertCache();

        [OperationContract]
        bool RemoveAlertCache(string orderId);

        [OperationContract]
        SessionEmails GetSessionAlertConfiguration(string SessionId);

        [OperationContract]
        bool AddSessionAlertConfiguration(SessionEmails sessionEmails);

        [OperationContract]
        bool UpdateSessionAlertConfiguration(SessionEmails sessionEmails);

        [OperationContract]
        bool DeleteSessionAlertConfiguration(string SessionId);

        [OperationContract]
        List<FixTagValueConfiguration> GetAllFixMessageConfiguration();

        [OperationContract]
        bool AddFixMessageConfiguration(FixTagValueConfiguration fixTagValueConfiguration);

        [OperationContract]
        bool DeleteFixMessageConfiguration(int id);

        [OperationContract]
        Stream GetFixMessageLogFileStream(string sessionId, string engineName);

        [OperationContract]
        bool FileExists(string sessionId, string engineName);

        [OperationContract]
        FIXEngine GetFixEngine(string engineID);

        [OperationContract]
        bool TcpConnection(string ipAddress, int port);

        [OperationContract]
        IEnumerable<string> GetSessionStatusMessage();

        [OperationContract]
        Task<string> TriggerJenkins(string branchName, string environment, string engineID);

        [OperationContract]
        Task<string> StartFixEngine(string engineID);

        [OperationContract]
        Task<string> StopFixEngine(string engineID);

        [OperationContract]
        Task<bool> AddJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration);
        
        [OperationContract]
        Task<bool> UpdateJenkinsConfiguration(FixEngineJenkinsConfiguration fixEngineJenkinsConfiguration);

        [OperationContract]
        Task<FixEngineJenkinsConfiguration> GetJenkinsConfiguration(string engineID);

        [OperationContract]
        Task<bool> DeleteJenkinsConfiguration(string engineID);

        [OperationContract]
        Task<IEnumerable<string>> GetJenkinsSlaveNodes();

        [OperationContract]
        Task<JenkinsJobStatus> GetJenkinsLatestJobStatus();

        [OperationContract]
        Task<EngineConfiguration> GetEngineConfiguration(string EngineId);

        [OperationContract]
        Task<bool> AddEngineConfiguration(EngineConfiguration engineConfiguration);

        [OperationContract]
        Task<bool> DeleteEngineConfiguration(string EngineId);

        [OperationContract]
        Task<List<EngineConfiguration>> GetAllEnginesConfiguration();
    }

    public interface IFIXMonitorServiceCallback
    {
       
        [OperationContract]
        void SendFixMessagesToClient(FIXMessage fixMessage, string engineID, string sessionID);

        [OperationContract]
        void SendFixMessageWithConfiguredFixTagValuePairToClient(FIXMessage fixMessage, string engineID, string sessionID);

        [OperationContract]
        void SendFixSessionToClient(FIXSession fixMessage, string engineID, string commandType);

        [OperationContract]
        void Heartbeat();

        [OperationContract]
        void SendAlertFlag(AlertFlag flag);

        [OperationContract]
        void SendFixSessionStatusMessage(string fixSessionStatusMessage);

        [OperationContract]
        void SendJenkinsJobUpdate(JenkinsJobStatus jenkinsJobStatus);

    }

    // Use a data contract as illustrated in the sample below to add composite types to service operations.
    // You can add XSD files into the project. After building the project, you can directly use the data types defined there, with the namespace "FIXMonitorService.ContractType".
    [DataContract]
    public class CompositeType
    {
        bool boolValue = true;
        string stringValue = "Hello ";

        [DataMember]
        public bool BoolValue
        {
            get { return boolValue; }
            set { boolValue = value; }
        }

        [DataMember]
        public string StringValue
        {
            get { return stringValue; }
            set { stringValue = value; }
        }

    }
}
