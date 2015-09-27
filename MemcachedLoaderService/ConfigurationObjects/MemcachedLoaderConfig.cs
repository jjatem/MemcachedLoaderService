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
        public int ReloadEntireCacheSeconds { get; set; }
        public MemcachedSettings MemcachedConnectionSettings { get; set; }
        public MySQLSettings MySQLConnectionSettings { get; set; }
        public List<CachedQuery> CachedQueriesCollection { get; set; }

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
                 * Load MySQL database connection settings - for now a single server support
                 */
                MySQLSettings MySqlConfig = new MySQLSettings();

                //server
                string dbServer = XmlDoc.SelectSingleNode("/configuration/mysql/server").InnerText;
                MySqlConfig.Server = dbServer;

                //port
                string dbPort = XmlDoc.SelectSingleNode("/configuration/mysql/port").InnerText;
                MySqlConfig.Port = dbPort;

                //username
                string dbUsername = XmlDoc.SelectSingleNode("/configuration/mysql/username").InnerText;
                MySqlConfig.Username = dbUsername;

                //password
                string dbPassword = XmlDoc.SelectSingleNode("/configuration/mysql/password").InnerText;
                MySqlConfig.Password = dbPassword;

                //password
                string dbName = XmlDoc.SelectSingleNode("/configuration/mysql/database").InnerText;
                MySqlConfig.Database = dbName;

                /*
                 * Load all objects in main configuration object
                 */
                config.MemcachedConnectionSettings = MemcachedServerSettings;
                config.MySQLConnectionSettings = MySqlConfig;
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
    }
}
