using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NMemcached.Client;
using Newtonsoft.Json;

namespace MemcachedLoaderServiceClient
{
    public class CacheLoaderServiceClient
    {
        public string MemcachedServer { get; set; }
        public int MemcachedPort { get; set; }

        public MemcachedClient Client
        {
            get
            {
                return client;
            }
        }

        private ServerConnectionCollection MemCachedServers = new ServerConnectionCollection();
        private MemcachedClient client;

        public CacheLoaderServiceClient(string server, int port)
        {
            this.MemcachedServer = server;
            this.MemcachedPort = port;

            /*
             * Add Server from Config Settings
             */
            this.MemCachedServers.Add(this.MemcachedServer, port: this.MemcachedPort);

            /*
             * Create the client
             */
            IConnectionProvider provider = new ConnectionProvider(MemCachedServers);
            this.client = new MemcachedClient(provider);
        }

        public string GetStoredJSONForKey(string key)
        {
            string retval = string.Empty;

            if (this.IsServerConnectionOpen())
            {
                return this.Client.Get(key).ToString();
            }

            return retval;
        }

        public bool IsServerConnectionOpen()
        {
            try
            {
                return (this.client != null && this.client.Versions().Count() > 0);
            }
            catch 
            {
                //don't do anything it will return as false
            }

            return false;
        }
    }
}
