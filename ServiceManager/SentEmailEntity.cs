using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EmpressServiceManager
{
    public class SentEmailEntity
    {
        public int SentEmailId { get; set; }
        public string EmailFrom { get; set; }
        public string EmailTo { get; set; }
        public string CcAddress { get; set; }
        public string Subject { get; set; }
        public string Body { get; set; }
        public string Attachment { get; set; }
        public int Status { get; set; }
        public DateTime MailCreateDate { get; set; }
        public DateTime MailSendDate { get; set; }
        
    }
}
