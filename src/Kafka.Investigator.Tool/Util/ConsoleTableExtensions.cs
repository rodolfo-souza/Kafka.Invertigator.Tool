using ConsoleTables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Util
{
    internal static class ConsoleTableExtensions
    {
        public static void WriteWithOptions(this ConsoleTable consoleTable, string? title = null, ConsoleColor? color = null, bool enableCount = false, Format format = Format.Default)
        {
            Console.WriteLine();

            var beforeColor = Console.ForegroundColor;

            if (color != null)
                Console.ForegroundColor = color.Value;

            if (!string.IsNullOrEmpty(title))
                new ConsoleTable($">>> {title}").Configure(c => c.EnableCount = false).Write(format);

            consoleTable.Options.EnableCount = enableCount;
            consoleTable.Write(format);
            
            Console.ForegroundColor = beforeColor;
        }
    }
}
