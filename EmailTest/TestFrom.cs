using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
 
using EmpressServiceManager;
using ServiceManager;

namespace EmailTest
{
    public partial class TestFrom : Form
    {
        public TestFrom()
        {
            InitializeComponent();
            AppConfigManager.ConfigAppSettings();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            //AbxSurverToInv pullService = new AbxSurverToInv();


            ////var interval = AppConfig.TimerInterval;
            ////pullService.LogWritter("Service run at : " + DateTime.Now);
            ////pullService.SyncData();
            //pullService.LogWritter("Service Run at: "+DateTime.Now);
            //pullService.SyncData();

            InvProductDataPullToAbx invService=new InvProductDataPullToAbx();
            invService.SyncData();


        }
    }
}
