using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace CascoDataParserBusiness
{
    public class CopyDataToDB
    {
        public CopyDataToDB()
        {

        }

        public static bool InsertDataTableintoSQLTableusingSQLBulkCopy(DataTable table, string tableName)
        {
            try
            {
                DataTable tableData = new DataTable();

                if (tableName.Contains("_vd_tbl"))
                {
                    tableName = "tblSupplierMaster";
                    tableData = Get_vd_tbl(table, tableName);
                }
                else if (tableName.Contains("_so_tbl"))
                {
                    tableName = "tblSalesOrders";
                    tableData = Get_so_tbl(table, tableName);
                }
                else if (tableName.Contains("_sh_tbl"))
                {
                    tableName = "tblShipToMaster";
                    tableData = Get_sh_tbl(table, tableName);
                }
                else if (tableName.Contains("_pt_tbl"))
                {
                    tableName = "tblPartMaster";
                    tableData = Get_pt_tbl(table, tableName);
                }
                else if (tableName.Contains("_po_tbl"))
                {
                    tableName = "tblSupplierSchedules";
                    tableData = Get_po_tbl(table, tableName);
                }
                else if (tableName.Contains("_ih_tbl"))
                {
                    tableName = "tblInvoiceHistory";
                    tableData = Get_ih_tbl(table, tableName);
                }
                else if (tableName.Contains("_cm_tbl"))
                {
                    tableName = "tblCustomerMaster";
                    tableData = Get_cm_tbl(table, tableName);
                }

                string conStr = ConfigurationManager.ConnectionStrings["CascoDBConnectionString"].ToString();

                using (SqlConnection sqlconnection = new SqlConnection(conStr))
                {
                    sqlconnection.Open();


                    // Copy the DataTable to SQL Server Table using SqlBulkCopy
                    using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(sqlconnection))
                    {
                        sqlBulkCopy.DestinationTableName = tableData.TableName;

                        foreach (var column in tableData.Columns)
                            sqlBulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());

                        sqlBulkCopy.WriteToServer(tableData);


                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return false;
            }
        }

        private static DataTable Get_ih_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();
                columns.Add("Domain", "string");
                columns.Add("Site", "string");
                columns.Add("SiteName", "string");
                columns.Add("PartNumber", "string");
                columns.Add("Description", "string");
                columns.Add("ProductLine", "string");
                columns.Add("ProductLineDescription", "string");
                columns.Add("BillToNumber", "string");
                columns.Add("BillToName", "string");
                columns.Add("CustomerNumber", "string");
                columns.Add("CustomerName", "string");
                columns.Add("ShipToNumber", "string");
                columns.Add("ShipToName", "string");
                columns.Add("InvoiceDate", "bool");
                columns.Add("QtyInvoiced", "string");
                columns.Add("UnitPrice", "string");
                columns.Add("SalesValues", "string");
                columns.Add("Currency", "string");
                columns.Add("CurrencyExchangeRate", "string");
                columns.Add("InvoiceType", "string");
                columns.Add("StandardMaterialCost", "string");
                columns.Add("StandardLaborCost", "string");
                columns.Add("StandardBurdenCost", "string");
                columns.Add("StandardOverheadCost", "string");
                columns.Add("StandardSubCost", "string");
                columns.Add("StandardTotalCost", "string");
                columns.Add("CurrentMaterialCost", "string");
                columns.Add("InvoiceNumber", "string");
                columns.Add("ShipperNumber", "string");
                columns.Add("SalesAccount", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["Site"] = row["Site"];
                    cascoRow["SiteName"] = row["Name"];
                    cascoRow["PartNumber"] = row["Part"];
                    cascoRow["Description"] = row["Description"];
                    cascoRow["ProductLine"] = row["Product Line"];
                    cascoRow["ProductLineDescription"] = row["Description_1"];
                    cascoRow["BillToNumber"] = row["Bill-to"];
                    cascoRow["BillToName"] = row["Name_1"];
                    cascoRow["CustomerNumber"] = row["Customer"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["CustomerName"] = row["Name_2"];
                    cascoRow["ShipToNumber"] = row["Ship-to"];
                    cascoRow["ShipToName"] = row["Name_3"];
                    cascoRow["InvoiceDate"] = row["Invoice Date"];
                    cascoRow["QtyInvoiced"] = row["Qty Invoiced"];
                    cascoRow["UnitPrice"] = row["Unit Price"];
                    cascoRow["SalesValues"] = row["Sales Value"];
                    cascoRow["Currency"] = row["Currency"];
                    cascoRow["CurrencyExchangeRate"] = row["Exch. Rate"];
                    cascoRow["InvoiceType"] = row["Inv Type"];
                    cascoRow["StandardMaterialCost"] = row["Std Mtl Cost"];
                    cascoRow["StandardLaborCost"] = row["Std Lbr Cost"];
                    cascoRow["StandardBurdenCost"] = row["Std Bdn Cost"];
                    cascoRow["StandardOverheadCost"] = row["Std Ovh Cost"];
                    cascoRow["StandardSubCost"] = row["Std Sub Cost"];
                    cascoRow["StandardTotalCost"] = row["Std Tot Cost"];
                    cascoRow["CurrentMaterialCost"] = row["Cur Mtl Cost"];
                    cascoRow["InvoiceNumber"] = row["Invoice Nbr"];
                    cascoRow["ShipperNumber"] = row["Shipper Nbr"];
                    cascoRow["SalesAccount"] = row["Sales Acct"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_cm_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();
                columns.Add("Domain", "string");
                columns.Add("CustomerNumber", "string");
                columns.Add("CustomerSortName", "string");
                columns.Add("CustomerName", "string");
                columns.Add("AddressLine1", "string");
                columns.Add("AddressLine2", "string");
                columns.Add("AddressLine3", "string");
                columns.Add("ZipCode", "string");
                columns.Add("City", "string");
                columns.Add("Country", "string");
                columns.Add("SupplierGroup", "string");
                columns.Add("SalesRepresentative", "string");
                columns.Add("CustomerArt", "string");
                columns.Add("Bank", "string");
                columns.Add("CreditTerms", "string");
                columns.Add("CreditLimit", "string");
                columns.Add("CreditTermsDescription", "string");
                columns.Add("DunsNumber", "string");
                columns.Add("VatNumber", "string");
                columns.Add("SupplierCodes", "string");
                columns.Add("Fob", "string");
                columns.Add("FobDescription", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["CustomerNumber"] = row["Customer"];
                    cascoRow["CustomerSortName"] = row["Sort"];
                    cascoRow["CustomerName"] = row["Name"];
                    cascoRow["AddressLine1"] = row["Address_line1"];
                    cascoRow["AddressLine2"] = row["Address_line2"];
                    cascoRow["AddressLine3"] = row["Address_line3"];
                    cascoRow["City"] = row["City"];
                    cascoRow["Country"] = row["Country"];
                    cascoRow["SalesRepresentative"] = row["Sales Rep"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["ZipCode"] = row["Zip Code"];
                    cascoRow["SupplierGroup"] = row["Supp. Group"];
                    cascoRow["Bank"] = row["Bank"];
                    cascoRow["CreditTerms"] = row["Credit Terms"];
                    cascoRow["CustomerArt"] = row["Cust. Art"];
                    cascoRow["CreditLimit"] = row["Credit Limits"];
                    cascoRow["CreditTermsDescription"] = row["Credit Desc."];
                    cascoRow["DunsNumber"] = row["Duns Number"];
                    cascoRow["VatNumber"] = row["VAT Number"];
                    cascoRow["SupplierCodes"] = row["Suppliercode"];
                    cascoRow["Fob"] = row["FOB"];
                    cascoRow["FobDescription"] = row["FOB Desc"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_vd_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();

                columns.Add("Domain", "string");
                columns.Add("SupplierNumber", "string");
                columns.Add("SupplierSortName", "string");
                columns.Add("SupplierName", "string");
                columns.Add("AddressLine1", "string");
                columns.Add("AddressLine2", "string");
                columns.Add("AddressLine3", "string");
                columns.Add("City", "string");
                columns.Add("Country", "string");
                columns.Add("SupplierGroup", "string");
                columns.Add("Buyer", "string");
                columns.Add("Bank", "string");
                columns.Add("CreditTerms", "string");
                columns.Add("CreditTermsDescription", "string");
                columns.Add("VatNumber", "string");
                columns.Add("ZipCode", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["SupplierNumber"] = row["Vendor"];
                    cascoRow["SupplierSortName"] = row["Sort"];
                    cascoRow["SupplierName"] = row["Name"];
                    cascoRow["AddressLine1"] = row["Address_line1"];
                    cascoRow["AddressLine2"] = row["Address_line2"];
                    cascoRow["AddressLine3"] = row["Address_line3"];
                    cascoRow["City"] = row["City"];
                    cascoRow["Country"] = row["Country"];
                    cascoRow["Buyer"] = row["Buyer"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["ZipCode"] = row["Zip Code"];
                    cascoRow["SupplierGroup"] = row["Supp. Group"];
                    cascoRow["Bank"] = row["Bank"];
                    cascoRow["CreditTerms"] = row["Credit Terms"];
                    cascoRow["CreditTermsDescription"] = row["Credit Desc."];
                    cascoRow["VatNumber"] = row["VAT Number"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_so_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();

                columns.Add("Domain", "string");
                columns.Add("Site", "string");
                columns.Add("OrderNumber", "string");
                columns.Add("Status", "string");
                columns.Add("Type", "string");
                columns.Add("BillToNumber", "string");
                columns.Add("ShipToNumber", "string");
                columns.Add("BillToName", "string");
                columns.Add("ItemNumber", "string");
                columns.Add("CustomerItemNumber", "string");
                columns.Add("PurchaseOrderNumber", "string");
                columns.Add("DueDate", "string");
                columns.Add("BackLog", "string");
                columns.Add("OpenQty", "string");
                columns.Add("DiscountPercent", "string");
                columns.Add("ListPrice", "string");
                columns.Add("MarkedPrice", "string");
                columns.Add("ExtendedPrice", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["Site"] = row["Site"];
                    cascoRow["OrderNumber"] = row["Order"];
                    cascoRow["BillToName"] = row["Name"];
                    cascoRow["Status"] = row["Stat"];
                    cascoRow["Type"] = row["Type"];
                    cascoRow["BillToNumber"] = row["Customer"];
                    cascoRow["ShipToNumber"] = row["Ship-to"];
                    cascoRow["ItemNumber"] = row["Item Number"];
                    cascoRow["CustomerItemNumber"] = row["Customer Item"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["PurchaseOrderNumber"] = row["Purchase Order"];
                    cascoRow["DueDate"] = row["Due"];
                    cascoRow["BackLog"] = row["Backlog"];
                    cascoRow["OpenQty"] = row["Open Qty"];
                    cascoRow["DiscountPercent"] = row["Disc %"];
                    cascoRow["ListPrice"] = row["List Price"];
                    cascoRow["MarkedPrice"] = row["Marked Price"];
                    cascoRow["ExtendedPrice"] = row["Ext Price"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_po_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();

                columns.Add("Domain", "string");
                columns.Add("Site", "string");
                columns.Add("OrderNumber", "string");
                columns.Add("Status", "string");
                columns.Add("Type", "string");
                columns.Add("SupplierNumber", "string");
                columns.Add("ShipTo", "string");
                columns.Add("SupplierName", "string");
                columns.Add("ItemNumber", "string");
                columns.Add("Buyer", "string");
                columns.Add("LineNumber", "string");
                columns.Add("ItemDescription", "string");
                columns.Add("SupplierItem", "string");
                columns.Add("OrderDate", "bool");
                columns.Add("DueDate", "bool");
                columns.Add("OpenQty", "string");
                columns.Add("Backlog", "string");
                columns.Add("ReleaseID", "string");
                columns.Add("CostPrice", "string");
                columns.Add("ExtendedPrice", "string");
                columns.Add("Currency", "string");
                columns.Add("FixedorPlan", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["Site"] = row["Site"];
                    cascoRow["OrderNumber"] = row["Order"];
                    cascoRow["SupplierName"] = row["Name"];
                    cascoRow["Status"] = row["Stat"];
                    cascoRow["Type"] = row["Type"];
                    cascoRow["SupplierNumber"] = row["Vendor"];
                    cascoRow["ShipTo"] = row["Ship-to"];
                    cascoRow["ItemNumber"] = row["Item Number"];
                    cascoRow["Buyer"] = row["Buyer"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["LineNumber"] = row["Line"];
                    cascoRow["ItemDescription"] = row["Description 1"];
                    cascoRow["SupplierItem"] = row["Vendor Item"];
                    cascoRow["OpenQty"] = row["Open Qty"];
                    cascoRow["OrderDate"] = row["Order Date"];
                    cascoRow["DueDate"] = row["Due Date"];
                    cascoRow["BackLog"] = row["Backlog"];
                    cascoRow["ReleaseID"] = row["Release ID"];
                    cascoRow["CostPrice"] = row["Cost Price"];
                    cascoRow["Currency"] = row["Curr"];
                    cascoRow["FixedorPlan"] = row["Fix/Plan"];
                    cascoRow["ExtendedPrice"] = row["Ext. Price"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_pt_tbl(DataTable table, string tableName)
        {
            try
            {
                Dictionary<string, string> columns = new Dictionary<string, string>();

                columns.Add("Domain", "string");
                columns.Add("Site", "string");
                columns.Add("Description", "string");
                columns.Add("StandardCost", "string");
                columns.Add("CurrentCost", "string");
                columns.Add("ItemType", "string");
                columns.Add("PartMenufactureCode", "string");
                columns.Add("SafetyStock", "string");
                columns.Add("ItemNumber", "string");
                columns.Add("SafetyTime", "string");
                columns.Add("PurchaseOrderLeadTime", "string");
                columns.Add("ProductionLeadTime", "string");
                columns.Add("DownloadDateTime", "bool");

                DataTable cascoDataTable = CreateTableStructure(columns, tableName);

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["Site"] = row["Site"];
                    cascoRow["Description"] = row["Desc"];
                    cascoRow["StandardCost"] = row["Std Cost"];
                    cascoRow["CurrentCost"] = row["Curr Cost"];
                    cascoRow["ItemType"] = row["Item Art"];
                    cascoRow["PartMenufactureCode"] = row["P/M Code"];
                    cascoRow["SafetyStock"] = row["Safety Stock"];
                    cascoRow["ItemNumber"] = row["Item Nbr."];
                    cascoRow["SafetyTime"] = row["Safety Time"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["PurchaseOrderLeadTime"] = row["PO Leadtime"];
                    cascoRow["ProductionLeadTime"] = row["Prod Leadtime"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable Get_sh_tbl(DataTable table, string tableName)
        {
            try
            {
                DataTable cascoDataTable = new DataTable(tableName);

                // Create Column 1: Domain
                DataColumn domainColumn = new DataColumn();
                domainColumn.ColumnName = "Domain";
                // Create Column 2: ShipTo
                DataColumn ShipToColumn = new DataColumn();
                ShipToColumn.ColumnName = "ShipToNumber";
                // Create Column 3: Sort
                DataColumn SortColumn = new DataColumn();
                SortColumn.ColumnName = "ShipToSortName";
                // Create Column 4: Name
                DataColumn NameColumn = new DataColumn();
                NameColumn.ColumnName = "ShipToName";
                // Create Column 5: Address1
                DataColumn Address1Column = new DataColumn();
                Address1Column.ColumnName = "AddressLine1";
                // Create Column 6: Address2
                DataColumn Address2Column = new DataColumn();
                Address2Column.ColumnName = "AddressLine2";
                // Create Column 7: Address3
                DataColumn Address3Column = new DataColumn();
                Address3Column.ColumnName = "AddressLine3";
                // Create Column 8: City
                DataColumn CityColumn = new DataColumn();
                CityColumn.ColumnName = "City";
                // Create Column 9: Country
                DataColumn CountryColumn = new DataColumn();
                CountryColumn.ColumnName = "Country";
                // Create Column 10: Customer
                DataColumn CustomerColumn = new DataColumn();
                CustomerColumn.ColumnName = "CustomerNumber";

                // Create Column 11: DownloadDateTime
                DataColumn DownloadDateTimeColumn = new DataColumn();
                DownloadDateTimeColumn.DataType = Type.GetType("System.DateTime");
                DownloadDateTimeColumn.ColumnName = "DownloadDateTime";

                // Create Column 12: Zipcode
                DataColumn ZipcodeColumn = new DataColumn();
                ZipcodeColumn.ColumnName = "ZipCode";



                // Add the columns to the cascoDataTable DataTable
                cascoDataTable.Columns.AddRange(new DataColumn[] { domainColumn, ShipToColumn,SortColumn,NameColumn,Address1Column,Address2Column,Address3Column,
             ZipcodeColumn,CityColumn,CountryColumn,CustomerColumn, DownloadDateTimeColumn });

                // Let's populate the datatable with our stats.
                // You can add as many rows as you want here by iterating!

                foreach (DataRow row in table.Rows)
                {
                    // Create a new row
                    DataRow cascoRow = cascoDataTable.NewRow();
                    cascoRow["Domain"] = row["Domain"];
                    cascoRow["ShipToNumber"] = row["Ship-to"];
                    cascoRow["ShipToSortName"] = row["Sort"];
                    cascoRow["ShipToName"] = row["Name"];
                    cascoRow["AddressLine1"] = row["Address_line1"];
                    cascoRow["AddressLine2"] = row["Address_line2"];
                    cascoRow["AddressLine3"] = row["Address_line3"];
                    cascoRow["City"] = row["City"];
                    cascoRow["Country"] = row["Country"];
                    cascoRow["CustomerNumber"] = row["Customer"];
                    cascoRow["DownloadDateTime"] = row["Download Date/Time"];
                    cascoRow["ZipCode"] = row["Zip Code"];

                    // Add the row to the Casco DataTable
                    cascoDataTable.Rows.Add(cascoRow);
                }


                return cascoDataTable;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return null;
            }
        }

        private static DataTable CreateTableStructure(Dictionary<string,string> columns, string tableName)
        {
            DataTable cascoDataTable = new DataTable(tableName);

            foreach (var name in columns)
            {
                DataColumn domainColumn = new DataColumn();
                domainColumn.ColumnName = name.Key;

                if(name.Value == "bool")
                    domainColumn.DataType = Type.GetType("System.DateTime");

                // Add the columns to the cascoDataTable DataTable
                cascoDataTable.Columns.Add(domainColumn );

            }

            return cascoDataTable;

        }
    }
}
