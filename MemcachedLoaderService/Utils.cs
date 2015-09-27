using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace MemcachedLoaderService
{
    public class Utils
    {
        public static EventLog GetEventLog()
        {
            EventLog eventLog = new EventLog("MemcachedLoaderConfig");
            return eventLog;
        }
    }
}
