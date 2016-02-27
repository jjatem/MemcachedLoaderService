using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using System.Diagnostics;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MemcachedLoaderServiceClient
{
    public class CacheLoaderRedisClient
    {

        #region properties

        public string RedisServer { get; set; }
        public int RedisPort { get; set; }
        public string RedisPassword { get; set; }

        public IRedisClientsManager RedisClientManager
        {
            get { return this.redisClientManager;  }
        }

        private IRedisClientsManager redisClientManager;
        private IRedisClient redisClient;

        /// <summary>
        /// Getter that opens the Redis Client Connection. If connection is already opened it returns the opened client instace
        /// </summary>
        public IRedisClient GetRedisClient
        {
            get
            {
                if (this.redisClient != null)
                {
                    return this.redisClient;
                }
                else
                {
                    this.OpenRedisConnection();                                        
                }

                return this.redisClient;
            }
        }

        #endregion

        #region ctor

        public CacheLoaderRedisClient(string Server, int Port, string Password = "")
        {
            /*
             * Initialize Redis Server connection properties
             */
            this.RedisServer = Server;
            this.RedisPort = Port;
            this.RedisPassword = Password;

            /*
             * Open a Redis Client Connection
             */
            this.OpenRedisConnection();

        }

        #endregion

        #region methods


        private void OpenRedisConnection()
        {
            /*
             * First build Redis connection string
             */
            string RedisConnectionString = this.GetConnectionString();

            /*
             * Then instantiate the client manager and return a client connection
             */
            using (this.redisClientManager = new PooledRedisClientManager(RedisConnectionString))
            {
                /*
                 * Get Redis Client
                 */
                this.redisClient = RedisClientManager.GetClient();
            }

        }


        private string GetConnectionString()
        {
            string connStr = string.Empty;

            if (!string.IsNullOrWhiteSpace(this.RedisServer) && this.RedisPort > 0)
            {
                if (string.IsNullOrWhiteSpace(this.RedisPassword))
                {
                    connStr = string.Format("redis://{0}:{1}", this.RedisServer, this.RedisPort);
                }
                else
                {
                    connStr = string.Format(@"redis://clientid:{0}@{1}:{2}", this.RedisPassword, this.RedisServer, this.RedisPort);
                }
            }
            else
            {
                throw new ApplicationException(string.Format("Invalid Redis Server Connection Settings. RedisHost [{0}]. RedisPort [{1}].", this.RedisServer, this.RedisPort));
            }

            return connStr;
        }

        #endregion

    }
}
