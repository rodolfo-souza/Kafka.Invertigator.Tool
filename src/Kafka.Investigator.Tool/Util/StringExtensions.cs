using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Util
{
    internal static class StringExtensions
    {
        public static string Limit(this string value, int maxLength, string trimIndicator = "")
        {
            if (value == null)
                return null;

            if (value.Length <= maxLength)
                return value;

            return value.Substring(0, maxLength) + trimIndicator;
        }
    }
}
