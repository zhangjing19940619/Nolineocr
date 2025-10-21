using System;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Serialization;
using System.Security;
using System.Xml.Linq;

namespace Deduce.DMIP.NoLineOcrParse.Exceptions
{
    public class RecoverableException : Exception
    {
        public RecoverableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    public class UnrecoverableException : Exception
    {
        public UnrecoverableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
