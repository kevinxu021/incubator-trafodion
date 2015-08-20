using System.Collections.Generic;
using System.Text;

namespace Trafodion.Data
{
    internal class TrafDbEncoder
    {
        private HPDbEncoding _transport;
        private ByteOrder _byteOrder;
        private Dictionary<HPDbEncoding, Encoding> _encodings;

        public TrafDbEncoder(ByteOrder bo)
        {
            _encodings = new Dictionary<HPDbEncoding, Encoding>(5);
            _encodings.Add(HPDbEncoding.Default, Encoding.Default);          
            _encodings.Add(HPDbEncoding.ISO88591, Encoding.GetEncoding("ISO8859-1"));
            _encodings.Add(HPDbEncoding.UCS2, Encoding.BigEndianUnicode);
            _encodings.Add(HPDbEncoding.MS932, Encoding.GetEncoding(932));
            _encodings.Add(HPDbEncoding.UTF8, Encoding.UTF8);

            this.ByteOrder = bo; //trigger property update
            this._transport = HPDbEncoding.UTF8; //trigger property update
        }

        public ByteOrder ByteOrder
        {
            get
            {
                return _byteOrder;
            }
            set
            {
                this._byteOrder = value;

                if (_byteOrder == ByteOrder.LittleEndian)
                {
                    //little endian
                    _encodings[HPDbEncoding.UCS2] =  Encoding.GetEncoding(1200);
                }
                else
                {
                    //big endian
                    _encodings[HPDbEncoding.UCS2] = Encoding.GetEncoding(1201);
                }
            }
        }

        public HPDbEncoding Transport
        {
            get
            {
                return _transport;
            }
        }

        public string GetString(byte[] buf, HPDbEncoding c)
        {
            return _encodings[c].GetString(buf);
        }

        public byte[] GetBytes(string s, HPDbEncoding c)
        {
            if (s == null)
            {
                return new byte[0];
            }

            return _encodings[c].GetBytes(s);
        }
    }
}
