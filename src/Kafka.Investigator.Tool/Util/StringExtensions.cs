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

        public static bool IsNullOrEmptyOrContaisSpaces(this string value)
        {
            if (string.IsNullOrEmpty(value))
                return true;

            if (value.Contains(" "))
                return true;

            return false;
        }

        public static bool ContainSpaces(this string value)
        {
            if (string.IsNullOrEmpty(value))
                return false;

            return value.Contains(" ");
        }
    }
}
