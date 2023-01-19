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

        public static string RequestUserResponse(string question, ConsoleColor? color = null, bool responseToUpper = true)
        {
            WriteWithColor(question, color);
            var userResponse = Console.ReadLine();

            if (responseToUpper)
                return userResponse.ToUpper();
            
            return userResponse;
        }
        
        public static void WriteDebug(string log)
        {
            Write(log);
        }

        public static void WriteInformation(string log)
        {
            WriteWithColor(log, ConsoleColor.Blue);
        }

        public static void WriteSuccess(string log)
        {
            WriteWithColor(log, ConsoleColor.Green);
        }

        public static void WriteWarning(string log)
        {
            WriteWithColor(log, ConsoleColor.Yellow);
        }

        public static void WriteError(string log)
        {
            WriteWithColor(log, ConsoleColor.Red);
        }

        public static void WriteWithColor(string log, ConsoleColor? color = null)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = color ?? beforeColor;
            Write(log);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteEmptyLine() => Write("", includeTime: false);

        private static void Write(string log, bool includeTime = true)
        {
            var time = includeTime ? DateTime.Now.ToString("HH:mm:ss") : "";
            if (includeTime)
                Console.WriteLine($"{time} > {log}");
            else
                Console.WriteLine($"{log}");
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
