using System.Collections.Generic;
using System.Text;

namespace EsgynDB.Data
{
    internal class EsgynDBEncoder
    {
        private EsgynDBEncoding _transport;
        private ByteOrder _byteOrder;
        private Dictionary<EsgynDBEncoding, Encoding> _encodings;

        public EsgynDBEncoder(ByteOrder bo)
        {
            _encodings = new Dictionary<EsgynDBEncoding, Encoding>(5);
            _encodings.Add(EsgynDBEncoding.Default, Encoding.Default);          
            _encodings.Add(EsgynDBEncoding.ISO88591, Encoding.GetEncoding("ISO8859-1"));
            _encodings.Add(EsgynDBEncoding.UCS2, Encoding.BigEndianUnicode);
            _encodings.Add(EsgynDBEncoding.MS932, Encoding.GetEncoding(932));
            _encodings.Add(EsgynDBEncoding.UTF8, Encoding.UTF8);

            this.ByteOrder = bo; //trigger property update
            this._transport = EsgynDBEncoding.UTF8; //trigger property update
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
                    _encodings[EsgynDBEncoding.UCS2] =  Encoding.GetEncoding(1200);
                }
                else
                {
                    //big endian
                    _encodings[EsgynDBEncoding.UCS2] = Encoding.GetEncoding(1201);
                }
            }
        }

        public EsgynDBEncoding Transport
        {
            get
            {
                return _transport;
            }
        }

        public string GetString(byte[] buf, EsgynDBEncoding c)
        {
            return _encodings[c].GetString(buf);
        }

        public byte[] GetBytes(string s, EsgynDBEncoding c)
        {
            if (s == null)
            {
                return new byte[0];
            }

            return _encodings[c].GetBytes(s);
        }
    }
}
