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
using System.Data.SqlClient;
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
        public static bool GetQueryCacheDictionaryFromDataTable(DatabaseSettings DBConfig, CachedQuery QuerySpecs, DataTable DataTableRowsToCache, out Dictionary<string,Dictionary<string, string>> DictionaryToCache, out string ErrorMessage)
        {
            bool CreatedDictionary = false;
            ErrorMessage = string.Empty;

            DictionaryToCache = new Dictionary<string, Dictionary<string, string>>();
            List<string> PKColumnNames = null;

            /*
             * First Get Database Table Schema for the table to cache
             */
            DataTable TableSchemaDefinition = Utils.GetSchemaTypeMySQLTable(DBConfig, QuerySpecs.DatabaseTableName);

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
            if (DataTableRowsToCache != null && DataTableRowsToCache.Rows.Count > 0)
            {
                foreach (DataRow dr in DataTableRowsToCache.Rows)
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
                DataTable QueryDataTable = GetMySQLTable(Config.DBConnectionSettings, QueryToLoad);

                /*
                 * Determine whether to permanently persist kvp cached object in Redis
                 */
                bool PersistCachedObject = (Config.MemcachedConnectionSettings.CacheObjectSeconds <= 0);

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
                    bool Success = Utils.GetQueryCacheDictionaryFromDataTable(Config.DBConnectionSettings, QueryToLoad, QueryDataTable, out MemoryDict, out ErrMsg);

                    /*
                     * Table Data Dictionary was successfully created - Cached each row in Memcached as a JSON dictionary
                     */
                    if (Success)
                    {
                        foreach (KeyValuePair<string, Dictionary<string, string>> TableDictionaryKvp in MemoryDict)
                        {
                            string Key = TableDictionaryKvp.Key;
                            string JsonStoreValue = JsonConvert.SerializeObject(TableDictionaryKvp.Value, new KeyValuePairConverter());

                            /*
                             * Determine right expiration Datetime value
                             */
                            DateTime ExpireDate = (PersistCachedObject) ? DateTime.MaxValue : DateTime.Now.AddSeconds(Config.MemcachedConnectionSettings.CacheObjectSeconds);


                            /*
                             * Load Kvp in Memcached
                             */
                            response = client.Set(Key, JsonStoreValue, ExpireDate);

                            /*
                             * If key already exists replace it
                             */
                            if (response == ResponseCode.KeyExists)
                            {
                                response = client.Replace(Key, JsonStoreValue, ExpireDate);
                            }
                        }
                    }
                    
                }

                /*
                 * Success
                 */
                LoadedQuery = (response == ResponseCode.NoError);
                Utils.GetEventLog().WriteEntry(string.Format("[MemcachedLoaderService.Memcached] Successfully loaded table [{0}] in the memory cache.", QueryToLoad.KeyPrefix));

            }
            catch (Exception ex)
            {
                ErrorMessage = string.Format("[MemcachedLoaderService.Memcached] Can't load query into Cache. Memcached Error Message [{0}].", ex.Message);
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
        public static string GetMySQLConnectionString(DatabaseSettings MySQLConfig)
        {
            if (MySQLConfig == null)
                throw new ApplicationException("Invalid MySQL Configuration Settings Object Instance. Cannot build a connection string.");

            return string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4};", MySQLConfig.Server, MySQLConfig.Port, MySQLConfig.Database, MySQLConfig.Username, MySQLConfig.Password);
        }

        /// <summary>
        /// Formats a SQL Server Connection String
        /// </summary>
        /// <param name="SqlServerConfig"></param>
        /// <returns></returns>
        public static string GetMSSQLServerConnectionString(DatabaseSettings SqlServerConfig)
        {
            if (SqlServerConfig == null)
                throw new ApplicationException("Invalid Microsoft Sql Server Configuration Settings Object Instance. Cannot build a connection string.");

            string FormattedPort = (!string.IsNullOrWhiteSpace(SqlServerConfig.Port)) ? ("," + SqlServerConfig.Port) : string.Empty;

            return string.Format("Address={0}{1};Database={2};User ID={3};Pwd={4};", SqlServerConfig.Server, FormattedPort, SqlServerConfig.Database, SqlServerConfig.Username, SqlServerConfig.Password);
        }


        /// <summary>
        /// Generic Get ADO.NET DataTable method
        /// </summary>
        /// <param name="DatabaseConfig"></param>
        /// <param name="MemCQuery"></param>
        /// <returns></returns>
        public static DataTable GetDataTable(DatabaseSettings DatabaseConfig, CachedQuery MemCQuery)
        {
            DataTable MyQueryTable = null;
            DBType DatabaseType;

            /*
             * First Determine whether to use the Main Database Connection Settings or use the Override Connection string of the Query. Override takes precedence over main
             */
            bool UseQueryDBConnectionString = (!string.IsNullOrWhiteSpace(MemCQuery.DBConnString) && MemCQuery.DBConnString.Contains("|"));

            /*
             * Determine Database Type
             */
            if (UseQueryDBConnectionString)
            {
                string[] DBConnArray = MemCQuery.DBConnString.Split('|');
                DatabaseType = DBTypesUtils.GetDBType(DBConnArray[0]);
            }
            else
            {
                DatabaseType = DBTypesUtils.GetDBType(DatabaseConfig.DBType);
            }

            /*
             * Use appropriate database retrieval logic based on DBType
             */
            switch (DatabaseType)
            {
                case DBType.MYSQL:
                    MyQueryTable = GetMySQLTable(DatabaseConfig, MemCQuery, UseQueryDBConnectionString);
                    break;
                case DBType.ORACLE:
                    MyQueryTable = GetMSSQLServerTable(DatabaseConfig, MemCQuery, UseQueryDBConnectionString);
                    break;
                case DBType.POSTGRESQL:
                    break;
                case DBType.SQLSERVER:
                    break;
                case DBType.UNSUPPORTED:
                    break;
                default:
                    break;
            }

            return MyQueryTable;
        }


        /// <summary>
        /// Retrieves a query from MySQL
        /// </summary>
        /// <param name="MySQLConfig"></param>
        /// <param name="MemCQuery"></param>
        /// <returns></returns>
        public static DataTable GetMySQLTable(DatabaseSettings MySQLConfig, CachedQuery MemCQuery, bool UseQueryDBOverride = false)
        {
            DataTable MyQueryTable = new DataTable();

            try
            {
                string ConnString = Utils.GetMySQLConnectionString(MySQLConfig);

                /*
                 * Use overriden query connection string if query specs has one
                 */
                if (UseQueryDBOverride) { ConnString = DBTypesUtils.GetDBTypeInfo(MemCQuery.DBConnString).ConnectionString; }

                /*
                 * Get Data Table logic using MySQL ADO.NET provider
                 */
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
        /// Retrieves a query from a Microsoft SQL Server
        /// </summary>
        /// <param name="SqlServerConfig"></param>
        /// <param name="MemCQuery"></param>
        /// <param name="UseQueryDBOverride"></param>
        /// <returns></returns>
        public static DataTable GetMSSQLServerTable(DatabaseSettings SqlServerConfig, CachedQuery MemCQuery, bool UseQueryDBOverride = false)
        {
            DataTable MyQueryTable = new DataTable();

            try
            {
                string ConnString = Utils.GetMSSQLServerConnectionString(SqlServerConfig);

                /*
                 * Use overriden query connection string if query specs has one
                 */
                if (UseQueryDBOverride) { ConnString = DBTypesUtils.GetDBTypeInfo(MemCQuery.DBConnString).ConnectionString; }

                /*
                 * Get Data Table logic using MySQL ADO.NET provider
                 */
                using (SqlConnection dbConn = new SqlConnection(ConnString))
                {
                    dbConn.Open();

                    SqlCommand MySqlServerCmd = new SqlCommand(MemCQuery.Sql, dbConn);
                    MySqlServerCmd.CommandType = CommandType.Text;
                    MySqlServerCmd.CommandTimeout = int.MaxValue;

                    SqlDataAdapter SqlServerAdapter = new SqlDataAdapter(MySqlServerCmd);
                    DataSet MyCachedQueryDataSet = new DataSet();
                    SqlServerAdapter.Fill(MyCachedQueryDataSet);

                    if (MyCachedQueryDataSet != null && MyCachedQueryDataSet.Tables != null && MyCachedQueryDataSet.Tables.Count > 0)
                    {
                        MyQueryTable = MyCachedQueryDataSet.Tables[0];
                    }

                    dbConn.Close();
                }
            }
            catch (Exception ex)
            {
                string ErrorMessage = string.Format("MemcachedLoaderService. Error Retrieving data from Microsoft Sql Server. Select Query is [{0}]. Error Message is [{1}].", MemCQuery.Sql, ex.Message);
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
        private static DataTable GetSchemaTypeMySQLTable(DatabaseSettings MySQLConfig, string TableName)
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
