// Copyright 2015 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Grpc.Core;
using Helloworld;
using System;

namespace GreeterClient
{
  class Program
    {
        public static void Main(string[] args)
        {
            Channel channel = new Channel("192.168.0.86:50051", ChannelCredentials.Insecure);

            //var client = new Greeter.GreeterClient(channel);
            var client2 = new FIXHubCommunicator.FIXHubCommunicatorClient(channel);
            client2.Connect(
              new ConnectRequest
              {
                SenderCompId = "192.168.10.2",
                TargetCompId = "192.168.20.2"
              }
              );
            Console.WriteLine("DONE");
            

            channel.ShutdownAsync().Wait();
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
    }
}
