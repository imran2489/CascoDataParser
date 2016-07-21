using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CascoDataParserBusiness;
using TestTextToDataSet;

namespace CascoDataParser
{
    public class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string folderPath = ConfigurationManager.AppSettings["FileFolder"];
                string archivePath = ConfigurationManager.AppSettings["ArchiveFolder"];               

                
                foreach (string file in Directory.EnumerateFiles(folderPath, "*.txt"))
                {
                    LogData.LogParse("Parsing of file: " + file + " started");

                    string[] pathArr = file.Split('\\');
                    string[] fileArr = pathArr.Last().Split('.');
                    string fileName = fileArr[0];
                    string[] pathArr1 = fileName.Split('_');
                    var pathArr2 = pathArr1.Take(pathArr1.Length - 1);
                    string finalName = string.Join("_", pathArr2);

                    DataTable cascoTable = TextToDataSet.Convertt(file, finalName, "|");

                    if (cascoTable != null)
                    {
                        bool status = CopyDataToDB.InsertDataTableintoSQLTableusingSQLBulkCopy(cascoTable, finalName);

                        LogData.LogParse("Data inserted into table");

                        if (status)
                        {
                            LogData.LogParse("Moving of file: " + file + " to Archive folder started");
                            MigrateFile.MoveReadFileToArchiveFolder(archivePath, file);

                            LogData.LogParse("Moving of file: " + file + " to Archive folder completed");
                        }
                    }

                }
                                                                      
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
            }
        }




       




    }
}
