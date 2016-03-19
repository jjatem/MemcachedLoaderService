using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace MemcachedLoaderService
{
    public class DBTypeInfo
    {
        public DBType DatabaseType { get; set; }
        public string ConnectionString { get; set; }
    }

    public enum DBType
    {
        MYSQL, 
        SQLSERVER,
        ORACLE,
        POSTGRESQL,
        UNSUPPORTED
    }

    public static class DBTypesUtils
    {
        /// <summary>
        /// Converts a string dbtype name to the proper enumeration type
        /// </summary>
        /// <param name="dbtype_name"></param>
        /// <returns></returns>
        public static DBType GetDBType(string dbtype_name)
        {
            DBType rv = DBType.UNSUPPORTED;

            try
            {
                rv = (DBType)Enum.Parse(typeof(DBType), dbtype_name.Trim().ToUpper());
            }
            catch
            {
                rv = DBType.UNSUPPORTED;
            }

            return rv;
        }

        /// <summary>
        /// Checks whether a string dbtype name is a valid member of the DBType enumeration
        /// </summary>
        /// <param name="dbtype_name"></param>
        /// <returns></returns>
        public static bool ValidDBType(string dbtype_name)
        {
            bool IsValid = false;

            Type EnumType = typeof(DBType);

            foreach(FieldInfo fi in EnumType.GetFields())
            {
                if (fi.Name.Equals(dbtype_name.Trim().ToUpper()))
                {
                    IsValid = true;
                    break;
                }
            }

            return IsValid;
        }

        public static DBTypeInfo GetDBTypeInfo(string config_dbconn_string)
        {
            DBTypeInfo rv = new DBTypeInfo();

            try
            {
                /*
                 * Connection string can't be blank
                 */
                if (string.IsNullOrWhiteSpace(config_dbconn_string) || !config_dbconn_string.Contains("|"))
                {
                    rv.DatabaseType = DBType.UNSUPPORTED;
                    rv.ConnectionString = string.Empty;
                    return rv;
                }
                /*
                 * Connection string must be in the proper format
                 */
                string[] DBConnStrArray = config_dbconn_string.Split('|');
                if (DBConnStrArray.Length != 2)
                {
                    rv.DatabaseType = DBType.UNSUPPORTED;
                    rv.ConnectionString = string.Empty;
                    return rv;
                }

                /*
                 * Return parsed values
                 */
                rv.DatabaseType = GetDBType(DBConnStrArray[0]);
                rv.ConnectionString = DBConnStrArray[1];

            }
            catch
            {
                rv.DatabaseType = DBType.UNSUPPORTED;
                rv.ConnectionString = string.Empty;
            }

            /*
             * Results
             */
            return rv;
        }
    }
}
