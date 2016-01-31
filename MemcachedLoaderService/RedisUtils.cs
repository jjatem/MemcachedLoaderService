using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using System.Diagnostics;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace MemcachedLoaderService
{
    public static class RedisUtils
    {
        public static EventLog GetEventLog()
        {
            EventLog eventLog = new EventLog("MemcachedLoaderConfig");
            eventLog.Source = "MemcachedLoaderConfig";
            return eventLog;
        }

        public static bool ReloadRedisServer(MemcachedLoaderConfig Configuration)
        {
            bool Refreshed = false;
            string ErrMsg = string.Empty;

            if (Configuration != null && Configuration.CachedQueriesCollection != null && Configuration.CachedQueriesCollection.Count > 0)
            {
                foreach (CachedQuery CacheQuery in Configuration.CachedQueriesCollection)
                {
                    if (!LoadQueryInRedis(Configuration, CacheQuery, out ErrMsg))
                    {
                        Utils.GetEventLog().WriteEntry(string.Format("MemcachedLoaderService.ReloadRedisServer. Error: {0}.", ErrMsg));
                    }
                }
            }

            /*
             * Return refresh results
             */
            return Refreshed;
        }

        public static bool LoadQueryInRedis(MemcachedLoaderConfig Config, CachedQuery QueryToLoad, out string ErrMsg)
        {
            bool LoadedQuery = false;
            ErrMsg = string.Empty;

            Dictionary<string, Dictionary<string, string>> MemoryDict;

            IRedisClientsManager RedisClientManager;
            string RedisConnectionString = Config.RedisConnectionSettings.GetConnectionString();

            try
            {
                using (RedisClientManager = new PooledRedisClientManager(RedisConnectionString))
                {
                    /*
                     * Get Redis Client
                     */
                    var client = RedisClientManager.GetClient();

                    /*
                     * Retrieve Query Data from MySql
                     */
                    DataTable QueryDataTable = Utils.GetMySQLTable(Config.MySQLConnectionSettings, QueryToLoad);

                    /*
                     * Cache each row from the data table as a JSON serialized dictionary into the Redis Cache Server
                     */
                    if (QueryDataTable != null && QueryDataTable.Rows.Count > 0)
                    {
                        //Define a dictionary to store the data table to be serialized into a JSON object
                        MemoryDict = null;
                        ErrMsg = string.Empty;

                        /*
                         * Convert DataTable / MySQL Query ResultSet in Dictionary<string,Dictionary<string,string>> object
                         */
                        bool Success = Utils.GetQueryCacheDictionaryFromDataTable(Config.MySQLConnectionSettings, QueryToLoad, QueryDataTable, out MemoryDict, out ErrMsg);

                        /*
                         * Table Data Dictionary was successfully created - Cached each row in Memcached as a JSON dictionary
                         */
                        if (Success)
                        {
                            foreach (KeyValuePair<string, Dictionary<string, string>> TableDictionaryKvp in MemoryDict)
                            {
                                string Key = TableDictionaryKvp.Key;
                                string JsonStoreValue = JsonConvert.SerializeObject(TableDictionaryKvp.Value, new KeyValuePairConverter());

                                LoadedQuery = client.Set<string>(Key, JsonStoreValue, DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds));

                                
                            }
                        }
                    }

                    /*
                     * Successfully loaded data table in Redis Cache
                     */
                    LoadedQuery = true;                    
                    Utils.GetEventLog().WriteEntry(string.Format("[MemcachedLoaderService.Redis] Successfully loaded table [{0}] in the memory cache.", QueryToLoad.KeyPrefix));


                }
            }
            catch (Exception ex)
            {
                LoadedQuery = false;
                ErrMsg = string.Format("[MemcachedLoaderService.Redis] Can't load query into Cache. Memcached Error Message [{0}].", ex.Message);
                Utils.GetEventLog().WriteEntry(ErrMsg);
            }

            return LoadedQuery;
        }
    }
}
