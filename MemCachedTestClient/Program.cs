using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NMemcached;
using NMemcached.Client;

namespace MemCachedTestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
                * Connect to memcached server
                */
            ServerConnectionCollection MemCachedServers = new ServerConnectionCollection();

            /*
             * Add Server from Config Settings
             */
            MemCachedServers.Add("192.168.1.48", 11211);

            /*
             * Create the client
             */
            IConnectionProvider provider = new ConnectionProvider(MemCachedServers);
            MemcachedClient client = new MemcachedClient(provider);

            System.Console.WriteLine("Dumping contents of cache...");
            System.Console.WriteLine("Printing California Customers table [calcustomers]");
            System.Console.WriteLine();

            ResponseCode response = ResponseCode.UnknownCommand;

            IDictionary<string, object> Responses = client.Get(new string[] { "calcustomers.key=1", "calcustomers.key=2", "calcustomers.key=3" });

            foreach (KeyValuePair<string, object> item in Responses)
            {
                System.Console.WriteLine("key: {0}, CustomerName: {1}", item.Key, item.Value.ToString());
            }

            System.Console.WriteLine("Press any key to exit....");
            System.Console.ReadLine();

        }
    }
}
