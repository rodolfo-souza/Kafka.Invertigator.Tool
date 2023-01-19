using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations
{
    internal static class UserInteractionsHelper
    {
        public static T? RequestInput<T>(string fieldName)
        {
            T? convertedValue = default;

            while (true)
            {
                Console.Write($"{fieldName}: ");

                if (TryConvert(Console.ReadLine(), out convertedValue))
                    break;
            }

            return convertedValue;
        }

        public static void WriteError(string log)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            WriteLine(log);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteWarning(string log)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            WriteLine(log);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteSuccess(string log)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Green;
            WriteLine(log);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteInformation(string log)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Blue;
            WriteLine(log);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteDebug(string log)
        {
            WriteLine(log);
        }

        private static void WriteLine(string log, bool includeTime = true)
        {
            var time = includeTime ? DateTime.Now.ToString("HH:mm:ss") : "";
            Console.WriteLine($"{time} > {log}");
        }

        private static bool TryConvert<T>(string value, out T? convertedValue)
        {
            convertedValue = default;

            if (string.IsNullOrEmpty(value))
                return true;

            try
            {
                convertedValue = (T)Convert.ChangeType(value, typeof(T));

                return true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Invalid input. {ex.Message}");
                return false;
            }
        }

        
    }
}
