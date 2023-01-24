using System.Security.Cryptography;
using System.Text;

namespace Kafka.Investigator.Tool.Util
{
    internal static class CryptographyExtensions
    {
        private const string KeyComlement = "e9b9174d-f445-45b1-8d62-3979fd0";

        public static string EncryptForUser(this string plainText)
            => Encrypt(plainText, Environment.UserName + Environment.UserDomainName);

        public static string DecryptForUser(this string plainText)
            => Decrypt(plainText, Environment.UserName + Environment.UserDomainName);

        public static string Encrypt(this string plainText, string key)
        {
            byte[] iv = new byte[16];
            byte[] array;

            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(CreateValidKey(key));
                aes.IV = iv;

                ICryptoTransform encryptor = aes.CreateEncryptor(aes.Key, aes.IV);

                using (MemoryStream memoryStream = new())
                {
                    using (CryptoStream cryptoStream = new(memoryStream, encryptor, CryptoStreamMode.Write))
                    {
                        using (StreamWriter streamWriter = new(cryptoStream))
                        {
                            streamWriter.Write(plainText);
                        }

                        array = memoryStream.ToArray();
                    }
                }
            }

            return Convert.ToBase64String(array);
        }

        public static string Decrypt(this string cipherText, string key)
        {
            byte[] iv = new byte[16];
            byte[] buffer = Convert.FromBase64String(cipherText);

            using (Aes aes = Aes.Create())
            {
                aes.Key = Encoding.UTF8.GetBytes(CreateValidKey(key));
                aes.IV = iv;
                ICryptoTransform decryptor = aes.CreateDecryptor(aes.Key, aes.IV);

                using (MemoryStream memoryStream = new(buffer))
                {
                    using (CryptoStream cryptoStream = new(memoryStream, decryptor, CryptoStreamMode.Read))
                    {
                        using (StreamReader streamReader = new(cryptoStream))
                        {
                            return streamReader.ReadToEnd();
                        }
                    }
                }
            }
        }

        private static string CreateValidKey(string key)
            => (key + KeyComlement).Substring(0, 32);
    }
}
