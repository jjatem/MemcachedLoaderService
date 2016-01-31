using System;
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


        /// <summary>
        /// Builds the appropriate Redis Server connection string
        /// for the ServiceStack.Redis Client
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            string connStr = string.Empty;

            if (!string.IsNullOrWhiteSpace(Server) && Port > 0)
            {
                if (string.IsNullOrWhiteSpace(Password))
                {
                    connStr = string.Format("redis://{0}:{1}", Server, Port);
                }
                else
                {
                    connStr = string.Format(@"redis://clientid:{0}@{1}:{2}", Password, Server, Port);
                }
            }

            return connStr;
        }


    }
}
