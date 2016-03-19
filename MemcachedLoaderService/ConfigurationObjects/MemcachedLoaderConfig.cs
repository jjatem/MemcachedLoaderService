using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Diagnostics;

namespace MemcachedLoaderService
{
    public class MemcachedLoaderConfig
    {
        #region properties

        public bool EnableMemcachedCaching { get; set; }
        public bool EnableRedisCaching { get; set; }
        public int ReloadEntireCacheSeconds { get; set; }
        public MemcachedSettings MemcachedConnectionSettings { get; set; }
        public RedisSettings RedisConnectionSettings { get; set; }
        public DatabaseSettings DBConnectionSettings { get; set; }
        public List<CachedQuery> CachedQueriesCollection { get; set; }


        #endregion

        #region methods

        public static MemcachedLoaderConfig LoadConfiguration(string XmlDocPath)
        {
            MemcachedLoaderConfig config = new MemcachedLoaderConfig();

            try
            {
                XmlDocument XmlDoc = new XmlDocument();
                XmlDoc.Load(XmlDocPath);

                /*
                 * Load cache service refresh/reload entire cache seconds internal. Service will reload all queries based on this interval
                 */
                XmlNode RefreshSeconds = XmlDoc.SelectSingleNode("/configuration/reload_entire_cache_seconds");
                config.ReloadEntireCacheSeconds = int.Parse(RefreshSeconds.InnerText);

                /*
                 * Enable Redis Caching Flag
                 */
                XmlNode EnableRedisCaching = XmlDoc.SelectSingleNode("/configuration/enable_redis_caching");
                config.EnableRedisCaching = bool.Parse(EnableRedisCaching.InnerText);

                /*
                 * Enable Memcached Caching
                 */
                XmlNode EnableMemcachedCaching = XmlDoc.SelectSingleNode("/configuration/enable_memcached_caching");
                config.EnableMemcachedCaching = bool.Parse(EnableMemcachedCaching.InnerText);

                /*
                 * Load memcached server connections settings
                 */
                MemcachedSettings MemcachedServerSettings = new MemcachedSettings();

                //server
                string mcServer = XmlDoc.SelectSingleNode("/configuration/memcached/server").InnerText;
                MemcachedServerSettings.Server = mcServer;

                //port
                int mcPort = int.Parse(XmlDoc.SelectSingleNode("/configuration/memcached/port").InnerText);
                MemcachedServerSettings.Port = mcPort;

                //cache items pin seconds
                int mcCacheItemExpireSeconds = int.Parse(XmlDoc.SelectSingleNode("/configuration/memcached/cache_object_seconds").InnerText);
                MemcachedServerSettings.CacheObjectSeconds = mcCacheItemExpireSeconds;

                /*
                 * Load Redis server connection settings
                 */
                RedisSettings RedisServerSettings = new RedisSettings();

                //redis server host
                string redisServer = XmlDoc.SelectSingleNode("/configuration/redis/server").InnerText;
                RedisServerSettings.Server = redisServer;

                //redis server port
                int redisPort = int.Parse(XmlDoc.SelectSingleNode("/configuration/redis/port").InnerText);
                RedisServerSettings.Port = redisPort;

                //redis server password
                string redisPassword = XmlDoc.SelectSingleNode("/configuration/redis/password").InnerText;
                RedisServerSettings.Password = redisPassword;

                //redis global cache object seconds setting
                int redisCacheItemExpireSeconds = int.Parse(XmlDoc.SelectSingleNode("/configuration/redis/cache_object_seconds").InnerText);
                RedisServerSettings.CacheObjectSeconds = redisCacheItemExpireSeconds;

                /*
                 * Load MySQL database connection settings - for now a single server support
                 */
                DatabaseSettings MySqlConfig = new DatabaseSettings();

                //server
                string dbServer = XmlDoc.SelectSingleNode("/configuration/database_settings/server").InnerText;
                MySqlConfig.Server = dbServer;

                //port
                string dbPort = XmlDoc.SelectSingleNode("/configuration/database_settings/port").InnerText;
                MySqlConfig.Port = dbPort;

                //username
                string dbUsername = XmlDoc.SelectSingleNode("/configuration/database_settings/username").InnerText;
                MySqlConfig.Username = dbUsername;

                //password
                string dbPassword = XmlDoc.SelectSingleNode("/configuration/database_settings/password").InnerText;
                MySqlConfig.Password = dbPassword;

                //password
                string dbName = XmlDoc.SelectSingleNode("/configuration/database_settings/database").InnerText;
                MySqlConfig.Database = dbName;

                /*
                 * Load all objects in main configuration object
                 */
                config.MemcachedConnectionSettings = MemcachedServerSettings;
                config.RedisConnectionSettings = RedisServerSettings;
                config.DBConnectionSettings = MySqlConfig;
                config.CachedQueriesCollection = LoadQueriesSettings(XmlDoc.SelectNodes("/configuration/cache_queries/query"));
            }
            catch (Exception ex)
            {
                EventLog eventLog = Utils.GetEventLog();
                string ErrorMessage = string.Format("MemcachedLoaderService. Error loading Service Configuration XML File. Error message was [{0}].", ex.Message);
                eventLog.WriteEntry(ErrorMessage);
                eventLog.Dispose();
                throw new ApplicationException(ErrorMessage);
            }

            /*
             * Returns fresh instance of configuration settings
             */
            return config;
        }


        private static List<CachedQuery> LoadQueriesSettings(XmlNodeList queriesNodes)
        {
            List<CachedQuery> ReturnCollection = new List<CachedQuery>();

            if (queriesNodes != null && queriesNodes.Count > 0)
            {
                foreach (XmlNode queryNode in queriesNodes)
                {
                    if (queryNode.HasChildNodes)
                    {
                        CachedQuery cachedQuery = new CachedQuery();

                        foreach (XmlNode XmlItem in queryNode.ChildNodes)
                        {
                            if (XmlItem.Name.Equals("keyprefix"))
                            {
                                cachedQuery.KeyPrefix = XmlItem.InnerText;
                                continue;
                            }
                            if (XmlItem.Name.Equals("sql"))
                            {
                                cachedQuery.Sql = XmlItem.InnerText;
                                continue;
                            }
                            if (XmlItem.Name.Equals("database_tablename"))
                            {
                                cachedQuery.DatabaseTableName = XmlItem.InnerText;
                                continue;
                            }
                            if (XmlItem.Name.Equals("db_connection"))
                            {
                                cachedQuery.DBConnString = XmlItem.InnerText;
                                continue;
                            }
                        }

                        /*
                         * Add new query to return collection
                         */
                        ReturnCollection.Add(cachedQuery);
                    }
                }
            }

            /*
             * Results
             */
            return ReturnCollection;
        }

        #endregion
    }
}
