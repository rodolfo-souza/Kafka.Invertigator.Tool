using Confluent.Kafka;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ExportMessageService
    {
        internal static void ExportMessage(Message<byte[], byte[]> message)
        {
            var stopAsk = false;
            while (!stopAsk)
            {
                try
                {
                    string? selectedDirectory = RequestDestinationDirectory();

                    if (!Directory.Exists(selectedDirectory))
                        Directory.CreateDirectory(selectedDirectory);

                    var filePrefix = UserInteractionsHelper.RequestInput<string>("Inform file name (ex.: naming 'abc' will create 'abc-key' and 'abc-value' files)");

                    var keyFilePath = Path.Combine(selectedDirectory, filePrefix + "-key");
                    var valueFilePath = Path.Combine(selectedDirectory, filePrefix + "-value");

                    if (!ConfirmExportEvenOverridden(keyFilePath))
                        return;

                    if (!ConfirmExportEvenOverridden(valueFilePath))
                        return;

                    File.WriteAllBytes(keyFilePath, message.Key);
                    File.WriteAllBytes(valueFilePath, message.Value);

                    UserInteractionsHelper.WriteSuccess($"Message exported sucessfully to: ");
                    UserInteractionsHelper.WriteSuccess(keyFilePath);
                    UserInteractionsHelper.WriteSuccess(valueFilePath);

                    stopAsk = true;
                }
                catch (Exception ex)
                {
                    UserInteractionsHelper.WriteError(ex.Message);

                    if (UserInteractionsHelper.RequestYesNoResponse("[Export message] Try again?") != "Y")
                        stopAsk = true;
                }
            }
        }

        private static bool ConfirmExportEvenOverridden(string filePath)
        {
            if (!File.Exists(filePath))
                return true;
            
            var replace = UserInteractionsHelper.RequestYesNoResponse($"File {filePath} already exists. Replace?");
            if (replace != "Y")
            { 
                UserInteractionsHelper.WriteWarning("Message export cancelled by user.");
                return false;
            }

            return true;
        }

        private static string RequestDestinationDirectory()
        {
            var defaultExportPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            defaultExportPath = Path.Combine(defaultExportPath, "kafkainvestigator_messages");

            var selectedDirectory = UserInteractionsHelper.RequestInput<string>($"Inform path (default {defaultExportPath})");

            if (string.IsNullOrEmpty(selectedDirectory))
                selectedDirectory = defaultExportPath;
            return selectedDirectory;
        }
    }
}
