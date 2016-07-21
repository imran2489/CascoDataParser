using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using TestTextToDataSet;
using CascoDataParserBusiness;
using System.Data.SqlClient;

namespace CascoDataParserUnitTest
{
    [TestClass]
    public class CascoTests
    {
        [TestMethod]
        public void T01_CascoDataParserTests_ReadFiles()
        {
            string folderPath = ConfigurationManager.AppSettings["FileFolder"];
            string connString = ConfigurationManager.ConnectionStrings["CascoDBConnectionString"].ToString();

            var files = Directory.EnumerateFiles(folderPath, "*.txt");
            Assert.IsNotNull(files);
            Assert.IsTrue(files.Count() >  0);            
        }

        [TestMethod]
        public void T02_CascoDataParserTests_ConvertTextDataToTable()
        {
            string folderPath = ConfigurationManager.AppSettings["FileFolder"];
            string connString = ConfigurationManager.ConnectionStrings["CascoDBConnectionString"].ToString();
            var files = Directory.EnumerateFiles(folderPath, "*.txt").ToList();
            Assert.IsNotNull(files);
            Assert.IsTrue(files.Count > 0);

            DataTable cascoTable = TextToDataSet.Convertt(files[0], "CascoTable", "|");

            Assert.IsNotNull(cascoTable);
            Assert.IsTrue(cascoTable.Rows.Count > 0);
        }

        [TestMethod]
        public void T03_CascoDataParserTests_InsertDatatoDatabase()
        {
            string folderPath = ConfigurationManager.AppSettings["FileFolder"];
            string connString = ConfigurationManager.ConnectionStrings["CascoDBConnectionString"].ToString();

            var files = Directory.EnumerateFiles(folderPath, "*.txt").ToList();
            Assert.IsNotNull(files);
            Assert.IsTrue(files.Count > 0);

            DataTable cascoTable = TextToDataSet.Convertt(files[0], "CascoTable", "|");

            Assert.IsNotNull(cascoTable);
            Assert.IsTrue(cascoTable.Rows.Count > 0);

            CopyDataToDB.InsertDataTableintoSQLTableusingSQLBulkCopy(cascoTable,"");


            string query = "select * from tblCustomerMaster";

            SqlConnection conn = new SqlConnection(connString);
            SqlCommand cmd = new SqlCommand(query, conn);
            conn.Open();

            // create data adapter
            SqlDataAdapter da = new SqlDataAdapter(cmd);
            // this will query your database and return the result to your datatable
            var dataTable = new DataTable();
            da.Fill(dataTable);
            conn.Close();
            da.Dispose();

            Assert.IsNotNull(dataTable);
            Assert.IsTrue(dataTable.Rows.Count > 0);
            
        }

        [TestMethod]
        public void T04_CascoDataParserTests_MigrateFileToArchiveFolder()
        {
            string folderPath = ConfigurationManager.AppSettings["FileFolder"];
            string connString = ConfigurationManager.ConnectionStrings["CascoDBConnectionString"].ToString();
            string archivePath = ConfigurationManager.AppSettings["ArchiveFolder"];

            var files = Directory.EnumerateFiles(folderPath, "*.txt").ToList();
            Assert.IsNotNull(files);
            Assert.IsTrue(files.Count > 0);

            var result = MigrateFile.MoveReadFileToArchiveFolder(archivePath, files[0]);
            Assert.IsTrue(result);
        }
    }
}
