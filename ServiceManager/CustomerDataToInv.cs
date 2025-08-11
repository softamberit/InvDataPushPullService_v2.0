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
    public class CustomerDataToInv
    {


        static string billgenixConnString = "";
        static string axConnString = "";
        static string InvConnString = "";

        public CustomerDataToInv()
        {
            axConnString = ConfigurationManager.ConnectionStrings["AbxConString"].ConnectionString;
            billgenixConnString = ConfigurationManager.ConnectionStrings["BillgenixConnectionString"].ConnectionString;
            InvConnString = ConfigurationManager.ConnectionStrings["InvPullConString"].ConnectionString;

        }
        public void SyncData()
        {

            try
            {

                LogWritter("Data Pulling from Abx");
                var dataAbx = GetCustomerInfoTableAbx();
                if (dataAbx != null && dataAbx.Rows.Count > 0)
                {
                    LogWritter("Total Customer Count: " + dataAbx.Rows.Count);

                }

                LogWritter("Data Pulling from Abx (Employee)");
                var dataAbxE = GetEmployeeInfoTableAbx();
                if (dataAbxE != null && dataAbxE.Rows.Count > 0)
                {
                    LogWritter("Total Employee Count: " + dataAbxE.Rows.Count);

                }

                LogWritter("Data Pulling from Bilgenix");
                var dataBillgenix = GetCustomerInfoTableBillgenix();
                if (dataBillgenix != null && dataBillgenix.Rows.Count > 0)
                {
                    LogWritter("Total Customer Count: " + dataBillgenix.Rows.Count);

                }

                BulkInsert(dataAbx, "TempAbxMigrationCustomer");
                BulkInsert(dataAbxE, "TempAbxMigrationEmployee");
                BulkInsert(dataBillgenix, "TempBillgenixMigrationCustomer");
                ProcessTempDataABX();
                ProcessTempDatabILLGENIX();
                ProcessTempDataABXEmployee();
                LogWritter("Data Synced");

            }
            catch (Exception ex)
            {

                LogWritter("Data Pulling error:" + ex.Message);

            }


        }

        private DataTable GetEmployeeInfoTableAbx()
        {
            var dt = new DataTable();
            using (var conn = new MySqlConnection(axConnString))
            {
                var query = @"SELECT EmployeeID, FullName,MobileNo,OfficialEmail,LocationName,
core_Designation.DesignationName FROM hr_Employment  
LEFT JOIN core_Location ON hr_Employment.LocationId=core_Location.LocationId 
LEFT JOIN core_Designation ON hr_Employment.FuncDesignationId=core_Designation.DesignationId
WHERE hr_Employment.EmployeeID!=111
";
                var adapter = new MySqlDataAdapter(query, conn);
                adapter.Fill(dt);

                //using (var command = new MySqlCommand(query,conn))
                //{

                //}
            }
            return dt;
        }

        private void ProcessTempDataABX()
        {
            string query = @"merge into AbxCustomerDetails as Target
                using TempAbxMigrationCustomer as Source
                on Target.CustomerId=Source.CustomerId and Target.LinkId=Source.LInkId
                when matched then update set 
				Target.LocationName=Source.LocationName,
                Target.CustomerId = Source.CustomerId,
                Target.CustomerName = Source.CustomerName,
                Target.LinkName = Source.LinkName,
                Target.LinkId = Source.LinkId,
                Target.CustomerAddress = Source.CustomerAddress,
                Target.Contact = Source.Contact
                when not matched then
                insert (LocationName,CustomerId,CustomerName,LinkName,LinkId,CustomerAddress,Contact) 
				values (Source.LocationName,Source.CustomerId,Source.CustomerName,
                Source.LinkName,Source.LinkId,Source.CustomerAddress,Source.Contact);";

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

        private void ProcessTempDataABXEmployee()
        {
            string query = @"merge into AbxEmployeeDetails as Target
                using TempAbxMigrationEmployee as Source
                on Target.EmployeeID=Source.EmployeeID
                when matched then update set 
				Target.FullName=Source.FullName,
                Target.MobileNo = Source.MobileNo,
                Target.OfficialEmail = Source.OfficialEmail,
                Target.LocationName = Source.LocationName,
                Target.DesignationName = Source.DesignationName
                when not matched then
                insert (EmployeeID,FullName,MobileNo,OfficialEmail,LocationName,DesignationName) 
				values (Source.EmployeeID,Source.FullName,Source.MobileNo,
                Source.OfficialEmail,Source.LocationName,Source.DesignationName);";

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
                CmnUserInsertSP();
            }

        }
        private void CmnUserInsertSP()
        {
            var connections = new SqlConnection(InvConnString);
            try
            {

                connections.Open();
                var comm = new SqlCommand("CmnUserAbxEmployeeInsert", connections);
                comm.CommandType = CommandType.StoredProcedure;
                comm.ExecuteNonQuery();
            }
            catch (Exception ex)
            {

                throw ex;
            }
            finally
            {
                connections.Close();

            }
        }

        private void ProcessTempDatabILLGENIX()
        {
            string query = @"merge into BillgenixCustomerDetails as Target
                using TempBillgenixMigrationCustomer as Source
                on Target.CustomerId=Source.CustomerId
                when matched then update set 
				Target.CustomerName=Source.CustomerName,
                Target.Address = Source.Address,
                Target.Mobile = Source.Mobile,
                Target.Email = Source.Email,
				Target.CustomerId = Source.CustomerId,
                Target.IsWithoutOnu  =Source.IsWithoutOnu
                when not matched then
                insert (CustomerName,Address,Mobile,Email,CustomerId,IsWithoutOnu) 
				values (Source.CustomerName,Source.Address,Source.Mobile,
                Source.Email,Source.CustomerId,Source.IsWithoutOnu);";

            var connection = new SqlConnection(InvConnString);

            try
            {
                //  LogWritter(string.Format("Total Records : {0} for {1} Table", "BillgenixCustomerDetails"));

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

        private DataTable GetCustomerInfoTableAbx()
        {
            var dt = new DataTable();
            using (var conn = new MySqlConnection(axConnString))
            {
                var query = @"SELECT sbm_CustomerLink.LocationName,sbm_CustomerLink.LinkId,CustomerId, CustomerName
,CONCAT(sbm_CustomerLink.LinkName,' ','(',sbm_CustomerLink.LinkId,')') AS LinkName,case when sbm_CustomerLink.SERVICEID in (16,3) THEN
sbm_Customer.CustomerAddress ELSE sbm_CustomerLinkService.Address end
AS CustomerAddress
,CONCAT(sbm_CustomerLinkService.ContactPersoneName,' ',sbm_CustomerLinkService.ContactNo) AS Contact

FROM sbm_CustomerLink
LEFT JOIN  sbm_Customer ON sbm_Customer.CustRecordId=sbm_CustomerLink.CustRecordId
LEFT JOIN SBM_CUSTOMERLINKSERVICE ON SBM_CUSTOMERLINK.LINKRECORDID = SBM_CUSTOMERLINKSERVICE.LINKRECORDID 
AND CASE WHEN SERVICEID=2 THEN SBM_CUSTOMERLINKSERVICE.LOCATIONTYPE=2 ELSE SBM_CUSTOMERLINKSERVICE.LOCATIONTYPE=1 END";
                var adapter = new MySqlDataAdapter(query, conn);
                adapter.Fill(dt);

                //using (var command = new MySqlCommand(query,conn))
                //{

                //}
            }
            return dt;
        }
        private DataTable GetCustomerInfoTableBillgenix()
        {
            try
            {
                var dt = new DataTable();
                using (var conn = new SqlConnection(billgenixConnString))
                {
                    var query = @"select CustomerID,CustomerName,Address,Mobile,Email,IsWithoutOnu from CustomerMaster";
                    var adapter = new SqlDataAdapter(query, conn);
                    adapter.Fill(dt);

                }
                return dt;

            }
            catch (Exception ex)
            {

                return null;

            }

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
