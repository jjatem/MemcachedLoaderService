using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemCachedTestClient
{
    public class Customer
    {
        public string id { get; set; }
        public string customer_name { get; set; }
        public string city { get; set; }
        public string region_state { get; set; }
        public string date_added { get; set; }
    }
}
