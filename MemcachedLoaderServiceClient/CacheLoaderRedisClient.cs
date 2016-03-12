using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
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

        public DataTable GetDataTableForCacheKeyPrefix(string key_prefix)
        {
            DataTable rv = null;

            List<Dictionary<string, string>> RowsDictionary = GetCachedRowsDictionaryCollectionForKeyPrefix(key_prefix);

            if (RowsDictionary != null && RowsDictionary.Count > 0)
            {
                rv = MemoryCacheClientUtils.GetDataTableFromDictionaries<string>(RowsDictionary);
            }

            return rv;
        }

        public List<Dictionary<string,string>> GetCachedRowsDictionaryCollectionForKeyPrefix(string key_prefix)
        {
            List<Dictionary<string, string>> ReturnCollection = new List<Dictionary<string, string>>();

            if (this.GetRedisClient != null)
            {
                List<string> AllStoredKeys = this.GetRedisClient.GetAllKeys();

                if (AllStoredKeys != null && AllStoredKeys.Count > 0)
                {
                    foreach (string StoredKey in AllStoredKeys)
                    {
                        if (StoredKey.Trim().ToUpper().StartsWith(key_prefix.Trim().ToUpper()))
                        {
                            ReturnCollection.Add(this.GetStoredRowDictionaryForKey(StoredKey));
                        }
                    }
                }
            }

            return ReturnCollection;
        }

        public string GetStoredJSONForKey(string key)
        {
            string retval = string.Empty;

            if (this.GetRedisClient != null)
            {
                return this.GetRedisClient.Get<string>(key);
            }

            return retval;
        }

        public Dictionary<string, string> GetStoredRowDictionaryForKey(string key)
        {
            Dictionary<string, string> retval = null;

            if (this.GetRedisClient != null)
            {
                string JSONDict = this.GetRedisClient.Get<string>(key);

                retval = JsonConvert.DeserializeObject<Dictionary<string, string>>(JSONDict);
            }

            return retval;
        }

        public string GetColumnValueForRowKeyandColumnName(string key, string column_name)
        {
            string retval = string.Empty;

            Dictionary<string, string> JSONDict = GetStoredRowDictionaryForKey(key);

            if (JSONDict != null && JSONDict.Count > 0)
            {
                if (JSONDict.ContainsKey(column_name))
                {
                    retval = JSONDict[column_name];
                }
            }

            return retval;
        }


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
