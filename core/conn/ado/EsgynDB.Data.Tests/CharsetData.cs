using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace TrafAdoTest
{
    class CharsetData
    {
        public List<string> unicode;
        public List<string> native;
        public List<int> byte_size;

        private string SJIS = "SJIS_CP932.TXT";
        private string GBK = "GBK_CP936.TXT";

        public CharsetData(string charset)
        {
            this.unicode = new List<string>();
            this.native = new List<string>();
            this.byte_size = new List<int>();

            switch (charset)
            {
                case "ASCII":
                    break;
                case "SJIS":
                    charset_load(this.SJIS);
                    break;
                case "GBK":
                case "GB2":
                    charset_load(this.GBK);
                    break;
                default:
                    Console.WriteLine("INTERNAL ERROR: No charset option found for {0}.", charset);
                    break;
            }
        }

        private void charset_load(string filename)
        {
            String strLine = "";
            char[] seps = { ' ', '\t'};

            try
            {
                Console.WriteLine("Loading character set ... {0}", filename);
                StreamReader sr = new StreamReader(filename);
                while ((strLine = sr.ReadLine()) != null)
                {
                    if (strLine.StartsWith("#")) continue;
                    string[] arr = strLine.Split(seps);
                    if (arr.Length < 2 || !arr[0].StartsWith("0x") || !arr[1].StartsWith("0x")) continue;
                    this.native.Add(arr[0].Substring(2));
                    this.unicode.Add(arr[1].Substring(2));
                }
                sr.Close();
                this.unicode.Reverse();
                this.native.Reverse();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public byte[] getNativeChars()
        {
            List<byte> arr = new List<byte>();

            foreach (string str in this.native)
            {
                if (str.Length == 2) {
                    arr.Add(Convert.ToByte(str, 16));
                }
                else if (str.Length == 4) {
                    arr.Add(Convert.ToByte(str.Substring(0,2), 16));
                    arr.Add(Convert.ToByte(str.Substring(2), 16));
                }
                else {
                    Console.WriteLine("Conversion string to bytes error: {0}", str);
                }
            }
            //char []str1 = Encoding.ASCII.GetChars(arr.ToArray());
            //int i = str1.Length;
            return arr.ToArray();
        }

        public string getUnicodeString()
         {
             string str = "";

             foreach (string aChar in this.unicode)
             {
                 if (aChar.Length == 4)
                 {
                     str += Convert.ToChar(Convert.ToInt64(aChar, 16));
                 }
                 else
                 {
                     Console.WriteLine("Conversion string to bytes error: {0}", aChar);
                 }
             }
             return str;
         }

         public string getUnicodeString(int index, int count)
         {
             string str = "";

             if ((index < 0) || (count < 1) ||
                 (index > this.unicode.Count) ||
                 ((index + count) > this.unicode.Count))
                 return str;

             for (int i = 0; i < count; i++ )
             {
                 string aChar = this.unicode[index + i];
                 if (aChar.Length == 4)
                 {
                     str += Convert.ToChar(Convert.ToInt64(aChar, 16));
                 }
                 else
                 {
                     Console.WriteLine("Conversion string to bytes error: {0}", aChar);
                 }
             }
             return str;
         }
     }
}
