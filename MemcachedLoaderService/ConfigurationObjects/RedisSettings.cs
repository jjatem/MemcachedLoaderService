﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService
{
    public class RedisSettings
    {
        public string Server { get; set; }
        public int Port { get; set; }
        public string Password { get; set; }
    }
}
