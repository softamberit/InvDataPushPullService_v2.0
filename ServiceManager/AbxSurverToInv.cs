using MySqlConnector;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Mail;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;


namespace ServiceManager
{
    public class AbxSurverToInv
    {


        static string axConnString = "";
        static string InvConnString = "";

        public AbxSurverToInv()
        {
            axConnString = ConfigurationManager.ConnectionStrings["AbxConString"].ConnectionString;
            InvConnString = ConfigurationManager.ConnectionStrings["InvPullConString"].ConnectionString;

        }
        public void SyncData()
        {

            try
            {

                LogWritter("Survey Data Pulling from Abx");
                var dataAbx = GetSurveyInfoAbx();
                if (dataAbx.Rows.Count > 0)
                {
                    LogWritter("Total Survey Count: " + dataAbx.Rows.Count);
                }


                BulkInsert(dataAbx, "TempAbxSurvey");
                ProcessTempDataABX();
                LogWritter("Data Synced");

            }
            catch (Exception ex)
            {

                LogWritter("Data Pulling error:" + ex.Message);

            }


        }

        private void ProcessTempDataABX()
        {
            string query = @"merge into AbxSurveyDetails as Target
                using TempAbxSurvey as Source
                on Target.LinkRecordId=Source.LinkRecordId and Target.LinkRecordId=Source.LinkRecordId
                when matched then update set 
				Target.LinkName=Source.LinkName,
                Target.ItemCode = Source.ItemCode,
                Target.LinkRecordId = Source.LinkRecordId,
                Target.LinkId = Source.LinkId,
                Target.MRC = Source.MRC,
                Target.OTC = Source.OTC,
                Target.Quantity = Source.Quantity,
                Target.Address = Source.Address,
                Target.LocationType = Source.LocationType
                when not matched then
                insert (LinkName,ItemCode,LinkRecordId,LinkId,MRC,OTC,Quantity,Address,LocationType) 
				values (Source.LinkName,Source.ItemCode,Source.LinkRecordId,
                Source.LinkId,Source.MRC,Source.OTC,Source.Quantity,Address,LocationType);";

            var connection = new SqlConnection(InvConnString);

            try
            {
                //LogWritter(string.Format("Total Records : {0} for {1} Table", "AbxCustomerDetails"));

                connection.Open();
                var comm = new SqlCommand();
                comm.Connection = connection;
                comm.CommandText = query;
                comm.CommandType = CommandType.Text;
                comm.ExecuteNonQuery();

            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                connection.Close();
            }

        }
   

        private DataTable GetSurveyInfoAbx()
        {
            var dt = new DataTable();
            using (var conn = new MySqlConnection(axConnString))
            {
                var query = @"SELECT SCL.LinkName,SCE.ItemCode,SCE.LinkRecordId,SCL.LinkId,SCL.OTC,SCL.MRC,SCE.Quantity, SCE.LocationType,SCLS.Address 
FROM sbm_CustomerEquipment SCE
INNER join sbm_CustomerLink SCL on SCL.LinkRecordId=SCE.LinkRecordId
left join sbm_CustomerLinkService SCLS on SCLS.LinkRecordId=SCL.LinkRecordId";
                var adapter = new MySqlDataAdapter(query, conn);
                adapter.Fill(dt);

                //using (var command = new MySqlCommand(query,conn))
                //{

                //}
            }
            return dt;
        }
       
        public DataTable ToDataTable<T>(IList<T> data)
        {
            PropertyDescriptorCollection props =
            TypeDescriptor.GetProperties(typeof(T));
            DataTable table = new DataTable();
            for (int i = 0; i < props.Count; i++)
            {
                PropertyDescriptor prop = props[i];
                table.Columns.Add(prop.Name, prop.PropertyType);
            }
            object[] values = new object[props.Count];
            foreach (T item in data)
            {
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = props[i].GetValue(item);
                }
                table.Rows.Add(values);
            }
            return table;
        }

        public void LogWritter(string message)
        {

            string filename = DateTime.Now.ToString("yyyy_MM_dd") + "_CustomerService.log";
            string appPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            int index = appPath.LastIndexOf("\\");
            appPath = appPath.Remove(index);
            string path = Path.Combine(appPath, "Log" + DateTime.Now.Year.ToString());
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }

            File.AppendAllText(path + "\\" + filename, "\r\n" + DateTime.Now.ToString("dd HH:mm:ss.f") + "\t" + message);

        }

        public void BulkInsert(DataTable dataTable, string tablname)
        {
            if (dataTable == null)
            {
                LogWritter(string.Format("Data Not found for {0} Table", tablname));

                return;
            }
            var connection = new SqlConnection(InvConnString);
            try
            {
                LogWritter(string.Format("Total Records : {0} for {1} Table", dataTable.Rows.Count, tablname));

                connection.Open();
                var comm = new SqlCommand();
                comm.Connection = connection;
                comm.CommandText = "Delete from " + tablname;
                comm.CommandType = CommandType.Text;
                comm.CommandTimeout = 300000;
                comm.ExecuteNonQuery();


                using (var bulk = new SqlBulkCopy(connection))
                {
                    bulk.DestinationTableName = tablname;
                    bulk.BulkCopyTimeout = 300000;
                    bulk.WriteToServer(dataTable);

                }

            }
            catch (Exception ex)
            {

                LogWritter(string.Format("{1} Table: {0}", ex.Message, tablname));

            }
            finally
            {
                connection.Close();
            }
        }




    }
}
