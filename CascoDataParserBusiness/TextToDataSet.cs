using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using CascoDataParserBusiness;

namespace TestTextToDataSet
{
    public class TextToDataSet
    {

        public TextToDataSet()
        { 
        }

        /// <summary>
        /// Converts a given delimited file into a dataset. 
        /// Assumes that the first line    
        /// of the text file contains the column names.
        /// </summary>
        /// <param name="File">The name of the file to open</param>    
        /// <param name="TableName">The name of the 
        /// Table to be made within the DataSet returned</param>
        /// <param name="delimiter">The string to delimit by</param>
        /// <returns></returns>  
        public static DataTable Convertt(string File, string TableName, string delimiter)
        {
            try
            {
                //The DataSet to Return
                DataSet result = new DataSet();

                //Open the file in a stream reader.
                StreamReader s = new StreamReader(File);



                //Split the first line into the columns       
                string[] columns = s.ReadLine().Split(delimiter.ToCharArray());

                //Add the new DataTable to the RecordSet
                result.Tables.Add(TableName);

                //Cycle the colums, adding those that don't exist yet 
                //and sequencing the one that do.
                foreach (string col in columns)
                {
                    bool added = false;
                    string next = "";
                    int i = 0;
                    while (!added)
                    {
                        //Build the column name and remove any unwanted characters.
                        string columnname = col + next;
                        columnname = columnname.Replace("#", "");
                        columnname = columnname.Replace("'", "");
                        columnname = columnname.Replace("&", "");

                        //See if the column already exists
                        if (!result.Tables[TableName].Columns.Contains(columnname))
                        {
                            //if it doesn't then we add it here and mark it as added
                            result.Tables[TableName].Columns.Add(columnname);
                            added = true;
                        }
                        else
                        {
                            //if it did exist then we increment the sequencer and try again.
                            i++;
                            next = "_" + i.ToString();
                        }
                    }
                }

                //Read the rest of the data in the file.        
                string AllData = s.ReadToEnd();

                s.Close();

                //Split off each row at the Carriage Return/Line Feed
                //Default line ending in most windows exports.  
                //You may have to edit this to match your particular file.
                //This will work for Excel, Access, etc. default exports.
                string[] rows = AllData.Split("\r\n".ToCharArray());

                //Now add each row to the DataSet        
                foreach (string r in rows)
                {
                    //Split the row at the delimiter.
                    string[] items = r.Split(delimiter.ToCharArray());

                    //Add the item
                    if (items != null && items.Count() > 1)
                        result.Tables[TableName].Rows.Add(items);
                }


                // Remove Null valued columns from data table
                DataTable table = result.Tables[0];

                table.Columns.Remove("Column1");


                foreach (DataRow row in table.Rows)
                {
                    var date = (string)row["Download Date/Time"];
                    var dtt = ConvertToDateTime(date);
                    row["Download Date/Time"] = dtt;


                    if (TableName.Contains("_po_tbl"))
                    {
                        var date2 = (string)row["Order Date"];
                        var dtt2 = ConvertToDateTime(date2);
                        row["Order Date"] = dtt2;

                        var date3 = (string)row["Due Date"];
                        var dtt3 = ConvertToDateTime(date3);
                        row["Due Date"] = dtt3;
                    }

                    if (TableName.Contains("_ih_tbl") )
                    {
                        var date2 = (string)row["Invoice Date"];
                        var dtt2 = ConvertToDateTime(date2);
                        row["Invoice Date"] = dtt2;

                      
                    }


                }

                //Return the imported data.        
                return table;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
            }

            return null;
        }

        private static DateTime ConvertToDateTime(string strDateTime)
        {

            DateTime dtFinaldate; string sDateTime;
            try { dtFinaldate = Convert.ToDateTime(strDateTime); }
            catch (Exception e)
            {
                string[] sDate = strDateTime.Split('/');
                sDateTime = sDate[1] + '/' + sDate[0] + '/' + sDate[2];
                dtFinaldate = Convert.ToDateTime(sDateTime);
            }
            return dtFinaldate;
        }
    }
}