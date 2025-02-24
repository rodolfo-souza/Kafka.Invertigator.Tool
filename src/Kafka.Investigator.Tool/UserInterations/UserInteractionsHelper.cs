﻿namespace Kafka.Investigator.Tool.UserInterations
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
            WriteWithColor(question, color: color, writeLine: false);

            var userResponse = Console.ReadLine();

            if (responseToUpper)
                return userResponse.ToUpper();
            
            return userResponse;
        }

        public static string RequestUserResponseKey(string text, ConsoleColor? color = null, bool responseToUpper = true)
        {
            WriteWithColor(text, color, writeLine: false);

            var userResponse = Console.ReadKey();
            
            if (!char.IsLetterOrDigit(userResponse.KeyChar))
            {
                WriteEmptyLine();
                WriteError("Invalid response (enter a letter or digit)");

                RequestUserResponseKey(text, color, responseToUpper);
            }

            string response = userResponse.KeyChar.ToString();

            if (responseToUpper)
                return response.ToUpper();

            WriteEmptyLine();

            return response;
        }

        public static string RequestYesNoResponse(string question, ConsoleColor? color = ConsoleColor.Yellow, bool responseToUpper = true)
        {
            return RequestUserResponse($"{question} [Y/N]: ", color, responseToUpper);
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

        public static void WriteWithColor(string log, ConsoleColor? color = null, bool writeLine = true, bool includeTime = true)
        {
            var beforeColor = Console.ForegroundColor;
            Console.ForegroundColor = color ?? beforeColor;
            Write(log, includeTime, writeLine);
            Console.ForegroundColor = beforeColor;
        }

        public static void WriteEmptyLine() => Write("", includeTime: false);

        private static void Write(string log, bool includeTime = true, bool writeLine = true)
        {
            var time = includeTime ? DateTime.Now.ToString("HH:mm:ss") : "";

            if (includeTime)
                log = $"{time} > {log}";

            if (writeLine)
                Console.WriteLine(log);
            else
                Console.Write(log);
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
