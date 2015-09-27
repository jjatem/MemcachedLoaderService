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

        public static bool ReloadMemcached(MemcachedLoaderConfig Configuration)
        {
            bool Refreshed = false;

            /*
             * Return refresh results
             */
            return Refreshed;
        }

        public static string GetMySQLConnectionString(MySQLSettings MySQLConfig)
        {
            if (MySQLConfig == null)
                throw new ApplicationException("Invalid MySQL Configuration Settings Object Instance. Cannot build a connection string.");

            return string.Format("Server={0};Port={1};Database={2};Uid={3};Pwd={4};", MySQLConfig.Server, MySQLConfig.Port, MySQLConfig.Database, MySQLConfig.Username, MySQLConfig.Password);
        }

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



                    dbConn.Close();
                }
            }
            catch (Exception ex)
            {

            }


            return MyQueryTable;
        }
    }
}
