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
    public class InvProductDataPullToAbx
    {

        static string connString = "";
        static string connStringAbx = "";
        

        public InvProductDataPullToAbx()
        {
            connString = ConfigurationManager.ConnectionStrings["InvPullConString"].ConnectionString;
            connStringAbx = ConfigurationManager.ConnectionStrings["AbxConString"].ConnectionString;
        }
        public void SyncData()
        {

            try
            {


                LogWritter("Data Pulling from Inventory");
                var data = GetProductInfoTable();
                if (data.Rows.Count > 0)
                {
                    LogWritter("Total Equipment Count: " + data.Rows.Count);

                }

                BulkInsert(data, "tmp_EquipmentData");

                ProcessTempData();
                LogWritter("Data Synced");

            }
            catch (Exception ex)
            {

                LogWritter("Data Pulling error:" + ex.Message);

            }


        }

        private void ProcessTempData()
        {
            string query = @"INSERT INTO sbm_EquipmentMaster (ItemGroupID,ItemName,ItemCode,ItemModelID,ItemBrandID,UOMID,ItemTypeID,ItemTypeName,BrandName,ModelName,UOMName,ItemGroupName,UnitPrice,LastUpdateDate,ItemSpecificationID,SpecificationName,SyncedDate,IsNotShow) 
                SELECT * FROM (SELECT ItemGroupID,ItemName,ItemCode,ItemModelID,ItemBrandID,UOMID,ItemTypeID,ItemTypeName,BrandName,ModelName,UOMName,ItemGroupName,UnitPrice AS tUnitPrice,LastUpdateDate AS tLastUpdateDate,ItemSpecificationID,SpecificationName,SyncedDate AS tSyncedDate,
                case when IsActive = 0 OR IsActive IS null then TRUE ELSE FALSE  END
                AS tIsNotShow FROM tmp_EquipmentData )S 
                ON DUPLICATE KEY UPDATE 
                UnitPrice = IF( (tUnitPrice > 0), tUnitPrice,UnitPrice), 
                LastUpdateDate = tLastUpdateDate,
                SyncedDate=tSyncedDate,
                IsNotShow=tIsNotShow;";



            var connection = new MySqlConnection(connStringAbx);

            try
            {

                connection.Open();
                var comm = new MySqlCommand();
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

        private DataTable GetProductInfoTable()
        {
            var dt = new DataTable();
            using (var conn = new SqlConnection(connString))
            {
                var query = @"with allItems as (
Select Barcode as ItemCode,
CmnItemMaster.ItemName + ' '+ isnull(S.SpecificationName,'')  +' ' + isnull(CmnItemModel.ModelName,'') +' '+ isnull(CmnItemBrand.BrandName,'') as ItemName,
CmnItemMaster.ItemGroupID, isnull(CmnItemMaster.ItemModelID,0)ItemModelID,isnull(CmnItemMaster.ItemBrandID,0)ItemBrandID,CmnItemMaster.UOMID,CmnItemMaster.ItemTypeID,CmnItemType.ItemTypeName, 
CmnItemBrand.BrandName,CmnItemModel.ModelName,CmnUOM.UOMName,  CmnItemGroup.ItemGroupName ,
0 as UnitPrice,CmnItemMaster.CreateOn as LastUpdateDate ,isnull(S.ItemSpecificationID,0)ItemSpecificationID ,S.SpecificationName,isnull(CmnItemMaster.IsActive,0)IsActive
from CmnItemMaster  
inner join CmnItemType on CmnItemType.ItemTypeID=CmnItemMaster.ItemTypeID 
left join CmnItemBrand on CmnItemBrand.ItemBrandID=CmnItemMaster.ItemBrandID  
left join CmnItemModel on CmnItemModel.ItemModelID=CmnItemMaster.ItemModelID  
inner join CmnUOM on CmnUOM.UOMID=CmnItemMaster.UOMID  
inner join CmnItemGroup on CmnItemGroup.ItemGroupID=CmnItemMaster.ItemGroupID  
left  JOIN CmnItemSpecification S ON S.ItemSpecificationID = CmnItemMaster.ItemSpecificationID
where  isnull(CmnItemMaster.IsDeleted,0)=0 and isnull(CmnItemMaster.IsActive,0)=1 )

, mrrItems as (
Select Barcode as ItemCode,
                    CmnItemMaster.ItemName + ' '+ isnull(S.SpecificationName,'')  +' ' + isnull(CmnItemModel.ModelName,'') +' '+ isnull(CmnItemBrand.BrandName,'') as ItemName,
                    CmnItemMaster.ItemGroupID, CmnItemMaster.ItemModelID,CmnItemMaster.ItemBrandID,CmnItemMaster.UOMID,CmnItemMaster.ItemTypeID,CmnItemType.ItemTypeName, 
                    CmnItemBrand.BrandName,CmnItemModel.ModelName,CmnUOM.UOMName,  CmnItemGroup.ItemGroupName ,
                    Max(UnitPrice)UnitPrice,Max(InvMrrDetail.CreateOn)LastUpdateDate ,S.ItemSpecificationID ,S.SpecificationName ,isnull(CmnItemMaster.IsActive,0) as IsActive
                    from CmnItemMaster  
                    inner join CmnItemType on CmnItemType.ItemTypeID=CmnItemMaster.ItemTypeID 
                    inner join CmnItemBrand on CmnItemBrand.ItemBrandID=CmnItemMaster.ItemBrandID  
                    inner join CmnItemModel on CmnItemModel.ItemModelID=CmnItemMaster.ItemModelID  
                    inner join CmnUOM on CmnUOM.UOMID=CmnItemMaster.UOMID  
                    inner join InvMrrDetail on InvMrrDetail.ItemID=CmnItemMaster.ItemID 
                    inner join CmnItemGroup on CmnItemGroup.ItemGroupID=CmnItemMaster.ItemGroupID  
                    inner  JOIN CmnItemSpecification S ON S.ItemSpecificationID = CmnItemMaster.ItemSpecificationID
                    where UnitPrice is not null and UnitPrice<>0 and InvMrrDetail.IsDeleted=0 and isnull(CmnItemMaster.IsDeleted,0)=0  
                    group by ItemName,Barcode,CmnItemMaster.ItemModelID,CmnItemMaster.ItemBrandID,CmnItemMaster.UOMID,CmnItemMaster.ItemTypeID,CmnItemType.ItemTypeName, 
                    CmnItemBrand.BrandName,CmnItemModel.ModelName,CmnUOM.UOMName, 
                    CmnItemGroup.ItemGroupName,CmnItemMaster.ItemGroupID,S.ItemSpecificationID ,S.SpecificationName,CmnItemMaster.IsActive
)

select allItems.ItemCode,
allItems.ItemName,
allItems.ItemGroupID, allItems.ItemModelID,allItems.ItemBrandID,allItems.UOMID,allItems.ItemTypeID,allItems.ItemTypeName, 
allItems.BrandName,allItems.ModelName,allItems.UOMName,  allItems.ItemGroupName ,
 case when mrrItems.UnitPrice>0 then  mrrItems.UnitPrice else allItems.UnitPrice end as UnitPrice,
case when mrrItems.LastUpdateDate>allItems.LastUpdateDate then mrrItems.LastUpdateDate else  allItems.LastUpdateDate end as LastUpdateDate ,

allItems.ItemSpecificationID ,allItems.SpecificationName, allItems.IsActive,   getdate() SyncedDate
from allItems
left join mrrItems on mrrItems.ItemCode=allItems.ItemCode";
                var adapter = new SqlDataAdapter(query, conn);
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

            string filename = DateTime.Now.ToString("yyyy_MM_dd") + "_serviceEquipment.log";
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
            var connection = new MySqlConnection(connStringAbx);
            try
            {
                LogWritter(string.Format("Total Records : {0} for {1} Table", dataTable.Rows.Count, tablname));

                connection.Open();
                var comm = new MySqlCommand();
                comm.Connection = connection;
                comm.CommandText = "Delete from " + tablname;
                comm.CommandType = CommandType.Text;
                comm.ExecuteNonQuery();
                comm.CommandTimeout = 1000 * 60 * 5;
                string sqlBulk = getInsertQuery(dataTable);

                comm.CommandText = sqlBulk;
                comm.ExecuteNonQuery();

                //var bulk = new MySqlBulkCopy(connection);


                //bulk.DestinationTableName = tablname;
                //bulk.WriteToServer(dataTable);

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

        private string getInsertQuery(DataTable dataTable)
        {
            string query = "";
            foreach (DataRow item in dataTable.Rows)
            {
                query += $@"INSERT INTO tmp_EquipmentData
                    (
                      ItemCode
                     ,ItemName
                     ,ItemGroupID
                     ,ItemModelID
                     ,ItemBrandID
                     ,UOMID
                     ,ItemTypeID
                     ,ItemTypeName
                     ,BrandName
                     ,ModelName
                     ,UOMName
                     ,ItemGroupName
                     ,UnitPrice
                     ,LastUpdateDate
                     ,ItemSpecificationID
                     ,SpecificationName
                     ,IsActive
                     ,SyncedDate
                    )
                    VALUES
                    (
                      '{item["ItemCode"]}'  
                     ,'{item["ItemName"]}' -- ItemName - VARCHAR(50)
                     ,'{item["ItemGroupID"]}' -- ItemGroupID - INT(11)
                     ,'{item["ItemModelID"]}' -- ItemModelID - INT(11)
                     ,'{item["ItemBrandID"]}' -- ItemBrandID - INT(11)
                     ,'{item["UOMID"]}' -- UOMID - INT(11)
                     ,'{item["ItemTypeID"]}' -- ItemTypeID - INT(11)
                     ,'{item["ItemTypeName"]}' -- ItemTypeName - VARCHAR(50)
                     ,'{item["BrandName"]}' -- BrandName - VARCHAR(50)
                     ,'{item["ModelName"]}'  -- ModelName - VARCHAR(50)
                     ,'{item["UOMName"]}' -- UOMName - VARCHAR(50)
                     ,'{item["ItemGroupName"]}' -- ItemGroupName - VARCHAR(50)
                     ,'{item["UnitPrice"]}' -- UnitPrice - DECIMAL(20, 6)
                     ,'{Convert.ToDateTime(item["LastUpdateDate"]).ToString("yyyy-MM-dd HH:mm:ss")}' -- LastUpdateDate - DATETIME
                     ,'{item["ItemSpecificationID"]}' -- ItemSpecificationID - INT(11)
                     ,'{item["SpecificationName"]}' -- SpecificationName - VARCHAR(50)
                     ,'{item["IsActive"]}' 
                     ,'{Convert.ToDateTime(item["SyncedDate"]).ToString("yyyy-MM-dd HH:mm:ss")}'

                    );";
            }
            return query;
        }

        public void BulkInsertMySQL(DataTable table, string tableName)
        {
            using (MySqlConnection connection = new MySqlConnection(connStringAbx))
            {
                connection.Open();

                using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.Transaction = tran;
                        cmd.CommandText = $"SELECT * FROM " + tableName + " limit 0";

                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            adapter.UpdateBatchSize = 10000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                            {
                                cb.SetAllValues = true;
                                adapter.Update(table);
                                tran.Commit();
                            }
                        };
                    }
                }
            }
        }


    }
}
