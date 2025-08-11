using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using EmpressServiceManager;
using ServiceManager;

namespace InvDataAutoSyncService
{
    partial class AbxDataSyncService : ServiceBase
    {
       // Timer sTimer = null;
        Timer sTimerABI = null;
        public AbxDataSyncService()
        {
            InitializeComponent();
        }

        //InvProductDataPullToAbx _invProdurctService = new InvProductDataPullToAbx();
        CustomerDataToInv _customerDetailDataService = new CustomerDataToInv();
        AbxSurverToInv _surverToInv = new AbxSurverToInv();

        protected override void OnStart(string[] args)
        {
            //AppConfigManager.ConfigAppSettings();
            //_invProdurctService.LogWritter("Service Started at : " + DateTime.Now);
            //sTimer = new Timer();
            //sTimer.Interval = Convert.ToInt32(ConfigurationManager.AppSettings["Interval"]) * 60000;
            //sTimer.Elapsed += new ElapsedEventHandler(sTimer_Elapsed);
            //sTimer.Enabled = true;
            //sTimer.Start();


            //for customer
            _customerDetailDataService.LogWritter("Service Started at (Customer): " + DateTime.Now);
          //  _invProdurctService.LogWritter("Service Started at (Product): " + DateTime.Now);
            _surverToInv.LogWritter("Service Started at (Survey): " + DateTime.Now);

            sTimerABI = new Timer();
            sTimerABI.Interval = Convert.ToInt32(ConfigurationManager.AppSettings["IntervalAbxBillgeToInventory"]) * 60000;
            sTimerABI.Elapsed += new ElapsedEventHandler(sTimer_ElapsedABI);
            sTimerABI.Enabled = true;
            sTimerABI.Start();



            base.OnStart(args);
           // _invProdurctService.SyncData();

        }

        //void sTimer_Elapsed(object sender, ElapsedEventArgs e)
        //{
        //    sTimer.Stop();
        //    try
        //    {
        //        _invProdurctService.SyncData();
        //    }
        //    catch (Exception ex)
        //    {
        //        _invProdurctService.LogWritter(ex.Message);


        //    }
        //    finally
        //    {
        //        sTimer.Start();
        //    }

        //    //EmailServiceManager.SendEmail();

        //    //ServiceManager.CheckForEmailAndSendEmail();

        //}
        
        void sTimer_ElapsedABI(object sender, ElapsedEventArgs e)
        {
            sTimerABI.Stop();
            try
            {
                _customerDetailDataService.SyncData();
                _surverToInv.SyncData();
              //  _invProdurctService.SyncData();
            }
            catch (Exception ex)
            {
                _customerDetailDataService.LogWritter(ex.Message);
                _surverToInv.LogWritter(ex.Message);
               // _invProdurctService.LogWritter(ex.Message);


            }
            finally
            {
                sTimerABI.Start();
            }

            //EmailServiceManager.SendEmail();

            //ServiceManager.CheckForEmailAndSendEmail();

        }

        protected override void OnStop()
        {
            //sTimer.Stop();
            sTimerABI.Stop();
            // TODO: Add code here to perform any tear-down necessary to stop your service.
        }
    }
}
