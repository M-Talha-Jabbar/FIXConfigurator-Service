using FIXMonitorBusinessLogicLayer.DataModels;
using FIXMonitorBusinessLogicLayer.KeyedCollections;
using StackExchange.Redis;
using System;
using System.Collections.Generic;

namespace FIXMonitorBusinessLogicLayer.IHandler
{
    public interface IFixHandler
    {
        void LoadFIXEngines();
        void LoadFIXSessions();
        bool ConnectFixSessionAsync(FIXSession fixSession);
        bool DisconnectFixSession(FIXSession fixSession);
        bool SetSequenceNumber(FIXSession fixSession);
        bool ResetSequenceNumber(FIXSession fixSession);
        List<FIXMessage> GetFixMessages(string fixEngineID, string fixSessionConnectionID);
        FixEnginesKeyedCollection GetFixEngines();
        FIXEngine ConnectToFixEngine(FIXEngine fixEngine);
        FIXEngine DisconnectToFixEngine(FIXEngine fixEngine);
        FIXSession ConnectToFixSession(string engineID, FIXSession fixSession);
        List<Tuple<string, string, string>> ParseAndStoreFixMessage(string fixMessage);
        void SendFixSessionUpdates(FIXSession fixSession, string engineID, string updateType);
        void SendFixMessageUpdates(FIXMessage fixMessage, string engineID, string sessionID);
        string GetFixTagValue(string fixMessage, string tag);
        FixSessionKeyedCollection GetFixSession(string FixEngineID);
        FIXMessage getObjectFromFixMessage(string msg);
        void SessionUpdates(string key, HashEntry[] result,FIXEngine fixEngine);
        void GetFixMessagesFromRedis(ConnectionMultiplexer muxer, RedisChannel channel, RedisValue message, FIXEngine fixEngine);
    }
}
