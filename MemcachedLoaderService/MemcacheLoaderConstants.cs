using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;

namespace MemcachedLoaderService
{
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
    }
}
