using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MemCachedTestClient
{
    public class Film
    {
        public string film_id { get; set; }
        public string title { get; set; }
        public string description { get; set; }
        public string release_year { get; set; }
        public string language_id { get; set; }
        public string original_language_id { get; set; }
        public string rental_duration { get; set; }
        public string rental_rate { get; set; }
        public string length { get; set; }
        public string replacement_cost { get; set; }
        public string rating { get; set; }
        public string special_features { get; set; }
        public string last_update { get; set; }
    }
}
