using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CascoDataParserBusiness.Entities;
using Newtonsoft.Json;

namespace CascoDataParserBusiness
{
    public static class LogData
    {
       

        public static void LogParse(string message)
        {
            string logFilePath = ConfigurationManager.AppSettings["LogFilePath"];
            Logger logger = new Logger();
            logger.Id = Guid.NewGuid().ToString();
            logger.Message = message;
            logger.DateTime = DateTime.UtcNow;

            logFile(logger,logFilePath);
        }

        public static void LogError(Exception exception)
        {
            string logFilePath = ConfigurationManager.AppSettings["ErrorLogFilePath"];
            ErrorLogger logger = new ErrorLogger();
            logger.Id = Guid.NewGuid().ToString();
            logger.Message = exception.Message;
            logger.InnerException = exception.InnerException != null ? exception.InnerException.ToString(): string.Empty;
            logger.StackTrace = exception.StackTrace;
            logger.DateTime = DateTime.UtcNow;

            logFile(logger,logFilePath);
        }

        private static void logFile<T>(this T  logger,string filePath)
        {
            
            // Read existing json data
            var jsonData = System.IO.File.ReadAllText(filePath);
            // De-serialize to object or create new list
            var logList = JsonConvert.DeserializeObject<List<T>>(jsonData)
                                  ?? new List<T>();

            logList.Add(logger);

            // Update json data string
            jsonData = JsonConvert.SerializeObject(logList);
            System.IO.File.WriteAllText(filePath, jsonData);
        }
    }
}
