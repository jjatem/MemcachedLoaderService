using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService
{
    public class MemcachedSettings
    {
        public string Server { get; set; }
        public int Port { get; set; }
        public int CacheObjectSeconds { get; set; }        
    }
}
