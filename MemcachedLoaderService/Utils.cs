using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NMemcached;
using NMemcached.Client;
using MySql.Data.MySqlClient;

namespace MemcachedLoaderService
{
    public class Utils
    {
        public static EventLog GetEventLog()
        {
            EventLog eventLog = new EventLog("MemcachedLoaderConfig");
            eventLog.Source = "MemcachedLoaderConfig";
            return eventLog;
        }

        /// <summary>
        /// Reloads the entire MemCached cached. Reloads all queries.
        /// </summary>
        /// <param name="Configuration"></param>
        /// <returns></returns>
        public static bool ReloadMemcached(MemcachedLoaderConfig Configuration)
        {
            bool Refreshed = false;
            string ErrMsg = string.Empty;

            if (Configuration != null && Configuration.CachedQueriesCollection != null && Configuration.CachedQueriesCollection.Count > 0)
            {
                foreach(CachedQuery CacheQuery in Configuration.CachedQueriesCollection)
                {                    
                    if (!LoadQueryInMemCached(Configuration, CacheQuery, out ErrMsg))
                    {
                        Utils.GetEventLog().WriteEntry(string.Format("MemcachedLoaderService.ReloadMemcached. Error: {0}.", ErrMsg));
                    }
                }
            }

            /*
             * Return refresh results
             */
            return Refreshed;
        }


        public static bool LoadQueryInMemCached(MemcachedLoaderConfig Config, CachedQuery QueryToLoad, out string ErrorMessage)
        {
            bool LoadedQuery = false;
            ErrorMessage = string.Empty;

            ResponseCode response = ResponseCode.NoError;

            try
            {
                /*
                 * Connect to memcached server
                 */
                ServerConnectionCollection MemCachedServers = new ServerConnectionCollection();

                /*
                 * Add Server from Config Settings
                 */
                MemCachedServers.Add(Config.MemcachedConnectionSettings.Server, port: Config.MemcachedConnectionSettings.Port);

                /*
                 * Create the client
                 */
                IConnectionProvider provider = new ConnectionProvider(MemCachedServers);
                MemcachedClient client = new MemcachedClient(provider);

                /*
                 * Retrieve Query Data from MySql
                 */
                DataTable QueryDataTable = GetMySQLTable(Config.MySQLConnectionSettings, QueryToLoad);

                /*
                 * Cache data
                 */
                if (QueryDataTable != null && QueryDataTable.Rows.Count > 0)
                {
                    foreach (DataRow dr in QueryDataTable.Rows)
                    {
                        //TODO: for now assume the pk is the first column. Need to change to dynamically determine PK
                        string Key = string.Format("{0}.key={1}", QueryToLoad.KeyPrefix, dr[0].ToString());

                        response = client.Set(Key, dr["customer_name"].ToString(), DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds));

                        if (response == ResponseCode.KeyExists)
                        {
                            response = client.Replace(Key, dr["customer_name"].ToString(), DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds));
                        }
                    }
                }

                /*
                 * Success
                 */
                LoadedQuery = (response == ResponseCode.NoError);
                Utils.GetEventLog().WriteEntry(string.Format("[MemcachedLoaderService] Successfully loaded table [{0}] in the memory cache.", QueryToLoad.KeyPrefix));

            }
            catch (Exception ex)
            {
                ErrorMessage = string.Format("Can't load query into Cache. Memcached Error Message [{0}].", ex.Message);
                Utils.GetEventLog().WriteEntry(ErrorMessage);
            }

            /*
             * Results
             */
            return LoadedQuery;
        }

        /// <summary>
        /// Formats a MySQL Connection String 
        /// </summary>
        /// <param name="MySQLConfig"></param>
        /// <returns></returns>
        public static string GetMySQLConnectionString(MySQLSettings MySQLConfig)
        {
            if (MySQLConfig == null)
                throw new ApplicationException("Invalid MySQL Configuration Settings Object Instance. Cannot build a connection string.");

            return string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4};", MySQLConfig.Server, MySQLConfig.Port, MySQLConfig.Database, MySQLConfig.Username, MySQLConfig.Password);
        }

        /// <summary>
        /// Retrieves a query from MySQL
        /// </summary>
        /// <param name="MySQLConfig"></param>
        /// <param name="MemCQuery"></param>
        /// <returns></returns>
        public static DataTable GetMySQLTable(MySQLSettings MySQLConfig, CachedQuery MemCQuery)
        {
            DataTable MyQueryTable = new DataTable();

            try
            {
                string ConnString = Utils.GetMySQLConnectionString(MySQLConfig);

                using (MySqlConnection dbConn = new MySqlConnection(ConnString))
                {
                    dbConn.Open();

                    MySqlCommand MySqlCmd = new MySqlCommand(MemCQuery.Sql, dbConn);
                    MySqlCmd.CommandType = CommandType.Text;
                    MySqlCmd.CommandTimeout = int.MaxValue;

                    MySqlDataAdapter MySqlAdapter = new MySqlDataAdapter(MySqlCmd);
                    DataSet MyCachedQueryDataSet = new DataSet();
                    MySqlAdapter.Fill(MyCachedQueryDataSet);

                    if (MyCachedQueryDataSet != null && MyCachedQueryDataSet.Tables != null && MyCachedQueryDataSet.Tables.Count > 0)
                    {
                        MyQueryTable = MyCachedQueryDataSet.Tables[0];
                    }

                    dbConn.Close();
                }
            }
            catch (Exception ex)
            {
                string ErrorMessage = string.Format("MemcachedLoaderService. Error Retrieving data from MySQL. Select Query is [{0}]. Error Message is [{1}].", MemCQuery.Sql, ex.Message);
                Utils.GetEventLog().WriteEntry(ErrorMessage);
            }

            /*
             * Return database table
             */
            return MyQueryTable;
        }
    }
}
