using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CascoDataParserBusiness.Entities
{
    public class Logger
    {
        public string Id { get; set; }
        public string Message { get; set; }

        public DateTime DateTime { get; set; }
    }

    public class ErrorLogger : Logger
    {
        public string StackTrace { get; set; }
        public string InnerException { get; set; }        
    }
}
