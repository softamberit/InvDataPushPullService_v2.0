using MySqlConnector;
using Newtonsoft.Json;
using System;
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
    public class AbxToBillgenixDataPullService
    {

        static string connString = "";
        static string connStringBillGenix = "";
        public AbxToBillgenixDataPullService()
        {
            connString = ConfigurationManager.ConnectionStrings["AbxConString"].ConnectionString;
            connStringBillGenix = ConfigurationManager.ConnectionStrings["BillgenixConnectionString"].ConnectionString;
        }
        public void SyncData()
        {



            var corpCustData = GetCorporateCustTable();

            BulkInsert(corpCustData, "Temp_Corp_Cust");

            var corpCustDataDtl = GetCorporateCustDetailsTable();
            BulkInsert(corpCustDataDtl, "Temp_Corp_Cust_Dtl");


            ProcessTempData();



        }

        

        private void ProcessTempData()
        {
            string query = @"merge into CorporateCustomer as Target
                using Temp_Corp_Cust as Source
                on Target.CompanyId=Source.CompanyId
                when matched then 
                update set Target.CompanyName=cast(rtrim(ltrim(Source.CompanyName)) as varchar(500)),
                Target.CompanyId = rtrim(ltrim(Source.CompanyId)),

                Target.Contact_Person = rtrim(ltrim(Source.Contact_Person)),

                Target.Contact_Phone = rtrim(ltrim(Source.Contact_Phone)),
                Target.Contact_Email = rtrim(ltrim(Source.Contact_Email)),
                Target.Status = Source.Status


                when not matched then
                insert (CompanyID,CompanyName,Contact_Person,Contact_Phone,Contact_Email,Status) values (rtrim(ltrim(Source.CompanyId)),cast(rtrim(ltrim(Source.CompanyName)) as varchar(500)), rtrim(ltrim(Source.Contact_Person)),
                rtrim(ltrim(Source.Contact_Phone)),rtrim(ltrim(Source.Contact_Email)),Source.Status);";


            query += @"merge into CorporateCustomerDetails as Target
                    using Temp_Corp_Cust_Dtl as Source
                    on Target.CircuitID=Source.CircuitID
                    when matched then 
                    update set 
                    Target.CompanyId = rtrim(ltrim(Source.CompanyId)),
                    Target.LocationName = rtrim(ltrim(Source.LocationName)),
                    Target.Address = cast(Source.Address as varchar(500))
                    when not matched then
                    insert (CircuitID,CompanyId,LocationName,Address) values (rtrim(ltrim(Source.CircuitID)),rtrim(ltrim(Source.CompanyId)), rtrim(ltrim(Source.LocationName)),
                    cast(Source.Address as varchar(500)));
                    ";

            var connection = new SqlConnection(connStringBillGenix);

            try
            {

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

        private DataTable GetCorporateCustDetailsTable()
        {
            var dt = new DataTable();
            using (var conn = new MySqlConnection(connString))
            {
                var query = @"Select clientservices.customerId as CompanyId,CircuitID,company_branch as LocationName,setup_address as Address from clientservices
                                inner join clients on clients.customerId=clientservices.customerId
                                where clients.Status=1";
                var adapter = new MySqlDataAdapter(query, conn);
                adapter.Fill(dt);

                //using (var command = new MySqlCommand(query,conn))
                //{

                //}
            }
            return dt;
        }



        private static DataTable GetCorporateCustTable()
        {
            var dt = new DataTable();
            using (var conn = new MySqlConnection(connString))
            {
                var query = @"Select  customerId as CompanyId, CompanyName,ContactPerson as Contact_Person,Mobile as   Contact_Phone,Email as Contact_Email, STATUS from clients where  STATUS=1";
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

            string filename = DateTime.Now.ToString("yyyy_MM_dd") + "_Emailservice.log";
            string appPath = System.Reflection.Assembly.GetExecutingAssembly().Location;
            int index = appPath.LastIndexOf("\\");
            appPath = appPath.Remove(index);
            string path = Path.Combine(appPath, "EmailLog" + DateTime.Now.Year.ToString());
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
            var connection = new SqlConnection(connStringBillGenix);
            try
            {
                LogWritter(string.Format("Total Records : {0} for {1} Table", dataTable.Rows.Count, tablname));

                connection.Open();
                var comm = new SqlCommand();
                comm.Connection = connection;
                comm.CommandText = "Delete from " + tablname;
                comm.CommandType = CommandType.Text;
                comm.ExecuteNonQuery();


                using (var bulk = new SqlBulkCopy(connection))
                {
                    bulk.DestinationTableName = tablname;
                    bulk.WriteToServer(dataTable);

                }
            }
            catch (Exception)
            {

                throw;
            }
            finally
            {
                connection.Close();
            }
        }


    }
}
