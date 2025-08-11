using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace EmpressServiceManager
{
    public static class AppConfig
    {

        public static int Port { get; set; }

        public static string Host { get; set; }
        public static string From { get; set; }
        public static string Password { get; set; }
        public static bool EnableSsl { get; set; }
        public static int TimeOut { get; set; }
        public static bool IsCredentials { get; set; }
        public static string FixedCcAddress { get; set; }


        public static string ConnectionString { get; set; }
        public static int TimerInterval{get;set;}

    }
}
