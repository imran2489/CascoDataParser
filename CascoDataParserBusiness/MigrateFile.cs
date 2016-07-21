using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CascoDataParserBusiness
{
    public static class MigrateFile
    {

        public static bool MoveReadFileToArchiveFolder(string archivePath, string file)
        {
            try
            { 
            string destFile = archivePath + Path.GetFileName(file);           

            System.IO.File.Copy(file, destFile, true);
            System.IO.File.Delete(file);
            return true;
            }
            catch (Exception ex)
            {
                LogData.LogError(ex);
                return false;
            }
        }
    }
}
