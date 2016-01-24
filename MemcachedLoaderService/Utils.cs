using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NMemcached;
using NMemcached.Client;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

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

        /// <summary>
        /// Loads the DataRows retrieved from MYSQL in a Generic Dictionary Ready to be Cached in MemCached. Uses Generic Collections but will have to be converted to a HashTable for the Memcached Client
        /// </summary>
        /// <param name="MySQLConfig"></param>
        /// <param name="QuerySpecs"></param>
        /// <param name="MySQLTableRowsToCache"></param>
        /// <param name="DictionaryToCache"></param>
        /// <param name="ErrorMessage"></param>
        /// <returns></returns>
        public static bool GetQueryCacheDictionaryFromDataTable(MySQLSettings MySQLConfig, CachedQuery QuerySpecs, DataTable MySQLTableRowsToCache, out Dictionary<string,Dictionary<string, string>> DictionaryToCache, out string ErrorMessage)
        {
            bool CreatedDictionary = false;
            ErrorMessage = string.Empty;

            DictionaryToCache = new Dictionary<string, Dictionary<string, string>>();
            List<string> PKColumnNames = null;

            /*
             * First Get Database Table Schema for the table to cache
             */
            DataTable TableSchemaDefinition = Utils.GetSchemaTypeMySQLTable(MySQLConfig, QuerySpecs.MySqlTableName);

            /*
             * Get Primary Key ColumnNames
             */
            if (TableSchemaDefinition.Columns != null && TableSchemaDefinition.Columns.Count > 0)
            {
                PKColumnNames = Utils.GetPrimaryKeyColumnNamesCollection(TableSchemaDefinition);
            }

            /*
             * Build in Memory Dictionary to Load in Memcached service
             */
            if (MySQLTableRowsToCache != null && MySQLTableRowsToCache.Rows.Count > 0)
            {
                foreach (DataRow dr in MySQLTableRowsToCache.Rows)
                {
                    string MainDictKey = dr.GetFormatedMemCachedKey(PKColumnNames, QuerySpecs);
                    Dictionary<string, string> NestedDictValue = dr.Table.Columns
                                    .Cast<DataColumn>()
                                    .ToDictionary(col => col.ColumnName, col => dr[col.ColumnName].ToString());

                    DictionaryToCache.Add(MainDictKey, NestedDictValue);
                }
            }

            /*
             * Determine whether dictionary was populated
             */
            CreatedDictionary = (DictionaryToCache != null && DictionaryToCache.Count > 0);

            return CreatedDictionary;
        }

        public static bool LoadQueryInMemCached(MemcachedLoaderConfig Config, CachedQuery QueryToLoad, out string ErrorMessage)
        {
            bool LoadedQuery = false;
            ErrorMessage = string.Empty;

            ResponseCode response = ResponseCode.UnknownCommand;

            Dictionary<string, Dictionary<string, string>> MemoryDict;

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
                 * Cache each row from the data table as a JSON serialized dictionary
                 */
                if (QueryDataTable != null && QueryDataTable.Rows.Count > 0)
                {
                    //Define a dictionary to store the data table to be serialized into a JSON object
                    MemoryDict = null;
                    string ErrMsg = string.Empty;

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

                            response = client.Set(Key, JsonStoreValue, DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds));

                            if (response == ResponseCode.KeyExists)
                            {
                                response = client.Replace(Key, JsonStoreValue, DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds));
                            }
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


        /// <summary>
        /// Get Table Schema. Needed to build PK simple or compounded key values
        /// </summary>
        /// <param name="MySQLConfig"></param>
        /// <param name="TableName"></param>
        /// <returns></returns>
        private static DataTable GetSchemaTypeMySQLTable(MySQLSettings MySQLConfig, string TableName)
        {
            DataTable ReturnTable = null;
            MySqlConnection MySqlConn = null;

            string ConnString = Utils.GetMySQLConnectionString(MySQLConfig);

            try
            {
                using (MySqlConn = new MySqlConnection(ConnString))
                {
                    MySqlCommand MySqlCmd = new MySqlCommand(TableName, MySqlConn);
                    MySqlCmd.CommandType = CommandType.TableDirect;

                    MySqlDataAdapter MySqlDataAdapt = new MySqlDataAdapter(MySqlCmd);

                    DataSet MyDataSet = new DataSet();
                    MySqlDataAdapt.FillSchema(MyDataSet, SchemaType.Source, TableName);

                    if (MyDataSet.Tables != null && MyDataSet.Tables.Count > 0)
                    {
                        ReturnTable = MyDataSet.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                string ErrorMessage = string.Format("MemcachedLoaderService. Error Retrieving Table Specs from MySQL Connection. TableName is [{0}]. Error Message is [{1}].", TableName, ex.Message);
                Utils.GetEventLog().WriteEntry(ErrorMessage);
            }
            finally
            {
                if (MySqlConn != null)
                    MySqlConn.Close();
            }

            return ReturnTable;
        }


        /// <summary>
        /// Gets the primary key columns for a given table schema
        /// </summary>
        /// <param name="TableSchemaDefinition"></param>
        /// <returns></returns>
        private static List<string> GetPrimaryKeyColumnNamesCollection(DataTable TableSchemaDefinition)
        {
            List<string> ReturnCollection = new List<string>();

            if (TableSchemaDefinition != null && TableSchemaDefinition.PrimaryKey != null && TableSchemaDefinition.PrimaryKey.Length > 0)
            {
                foreach (DataColumn ColumDef in TableSchemaDefinition.PrimaryKey)
                {
                   ReturnCollection.Add(ColumDef.ColumnName);
                }
            }

            return ReturnCollection;
        }
    }
}
