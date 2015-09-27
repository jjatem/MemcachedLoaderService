﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace MemcachedLoaderService
{
    public partial class MemcachedLoaderService : ServiceBase
    {
        protected MemcachedLoaderConfig Configuration { get; private set; }
        private static System.Timers.Timer RefreshTimer;

        public MemcachedLoaderService()
        {
            InitializeComponent();
            LoadConfiguration();
        }

        protected override void OnStart(string[] args)
        {
            /*
             * Instatiate and set timer interval
             */
            MemcachedLoaderService.RefreshTimer = new Timer();
            MemcachedLoaderService.RefreshTimer.Interval = this.Configuration.ReloadEntireCacheSeconds * 1000;

            // Hook up the Elapsed event for the timer. 
            MemcachedLoaderService.RefreshTimer.Elapsed += OnTimedEvent;

            // Have the timer fire repeated events (true is the default)
            MemcachedLoaderService.RefreshTimer.AutoReset = true;

            // Start the timer
            MemcachedLoaderService.RefreshTimer.Enabled = true;

            Utils.GetEventLog().WriteEntry("Successfully started MemcachedLoaderService.");
        }

        protected override void OnStop()
        {
            /*
             * Stop the timer and dispose objects
             */

            //dispose configuration object
            this.Configuration = null;

            //stop and dispose the refresh timer
            MemcachedLoaderService.RefreshTimer.Enabled = false;            
            MemcachedLoaderService.RefreshTimer.Dispose();
            MemcachedLoaderService.RefreshTimer = null;
        }

        private static void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)
        {
            Console.WriteLine("The Elapsed event was raised at {0}", e.SignalTime);
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
