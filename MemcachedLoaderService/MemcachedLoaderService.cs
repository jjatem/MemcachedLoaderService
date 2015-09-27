using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace MemcachedLoaderService
{
    public partial class MemcachedLoaderService : ServiceBase
    {
        protected MemcachedLoaderConfig Configuration { get; private set; }

        public MemcachedLoaderService()
        {
            InitializeComponent();
            LoadConfiguration();
        }

        protected override void OnStart(string[] args)
        {
        }

        protected override void OnStop()
        {
        }

        private void LoadConfiguration()
        {
            /*
             * Load XML configuration file
             */
            string ConfigPath = AppDomain.CurrentDomain.BaseDirectory + "ServiceConfiguration.xml";
            this.Configuration = MemcachedLoaderConfig.LoadConfiguration(ConfigPath);
        }
    }
}
