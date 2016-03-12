using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using NMemcached;
using NMemcached.Client;
using MemcachedLoaderServiceClient;

namespace MemCachedTestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            //TestNMemcachedClient();
            //TestMemcachedClientLogic();

            TestRedisClient();
           

            System.Console.WriteLine("Press Any Key to Exit...");
            System.Console.ReadKey();

        }

        public static void TestRedisClient()
        {
            CacheLoaderRedisClient RedisCacheServer = new CacheLoaderRedisClient("192.168.1.48", 6379);

            System.Console.WriteLine(RedisCacheServer.GetStoredJSONForKey("customer.key=1"));

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine(RedisCacheServer.GetStoredJSONForKey("calcustomers.key=2"));

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine("Obtaining row dictionary using Redis Client Get Row Dictionary Method");
            Dictionary<string, string> JSONDict = RedisCacheServer.GetStoredRowDictionaryForKey("calcustomers.key=1");
            System.Console.WriteLine("Success obtaining Row Dictionary...");

            System.Console.WriteLine("Obtaining complete rows dictionary objects using Redis Client Get Row Dictionary Method");
            List<Dictionary<string, string>> JSONDictCollection = RedisCacheServer.GetCachedRowsDictionaryCollectionForKeyPrefix("customer");
            System.Console.WriteLine("Success obtaining Row Dictionary...");

            System.Console.WriteLine("Obtaining complete Data table with row objects using Redis Client Get Data Table Method");
            DataTable MyCachedTable = RedisCacheServer.GetDataTableForCacheKeyPrefix("customer");
            System.Console.WriteLine("Success obtaining DataTable with cached rows...");

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine("Obtaining City Name for Customer with Id 3");
            System.Console.WriteLine("City Name is: {0}", RedisCacheServer.GetColumnValueForRowKeyandColumnName("customer.key=3", "city"));

            System.Console.WriteLine();
            System.Console.WriteLine();

        }

        public static void TestMemcachedClientLogic()
        {
            CacheLoaderMemcachedClient MemcachedServer = new CacheLoaderMemcachedClient("192.168.1.48", 11211);

            if (!MemcachedServer.IsServerConnectionOpen())
            {
                System.Console.WriteLine("Could not connect to Memcached server host: [{0}] on port: [{1}]. Check your logs and network connections.", MemcachedServer.MemcachedServer, MemcachedServer.MemcachedPort);
                System.Console.WriteLine("Press Any Key to Exit...");
                System.Console.ReadKey();
                return;
            }

            System.Console.WriteLine(MemcachedServer.GetStoredJSONForKey("customer.key=1"));

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine(MemcachedServer.GetStoredJSONForKey("calcustomers.key=2"));

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine("Obtaining dictionary using Client Get Row Dictionary Method");
            Dictionary<string, string> JSONDict = MemcachedServer.GetStoredRowDictionaryForKey("calcustomers.key=1");
            System.Console.WriteLine("Success obtaining Row Dictionary...");

            System.Console.WriteLine();
            System.Console.WriteLine();

            System.Console.WriteLine("Obtaining City Name for Customer with Id 3");
            System.Console.WriteLine("City Name is: {0}", MemcachedServer.GetColumnValueForRowKeyandColumnName("customer.key=3", "city"));

            System.Console.WriteLine();
            System.Console.WriteLine();
        }

        public static void TestNMemcachedClient()
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
