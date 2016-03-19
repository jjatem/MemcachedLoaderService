using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService
{
    public class CachedQuery
    {
        public string DBConnString { get; set; }
        public string KeyPrefix { get; set; }
        public string Sql { get; set; }
        public string DatabaseTableName { get; set; }
    }
}
