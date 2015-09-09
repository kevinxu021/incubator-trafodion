using System.Collections.Generic;
using System.Text;

namespace Esgyndb.Data
{
    internal class EsgyndbEncoder
    {
        private EsgyndbEncoding _transport;
        private ByteOrder _byteOrder;
        private Dictionary<EsgyndbEncoding, Encoding> _encodings;

        public EsgyndbEncoder(ByteOrder bo)
        {
            _encodings = new Dictionary<EsgyndbEncoding, Encoding>(5);
            _encodings.Add(EsgyndbEncoding.Default, Encoding.Default);          
            _encodings.Add(EsgyndbEncoding.ISO88591, Encoding.GetEncoding("ISO8859-1"));
            _encodings.Add(EsgyndbEncoding.UCS2, Encoding.BigEndianUnicode);
            _encodings.Add(EsgyndbEncoding.MS932, Encoding.GetEncoding(932));
            _encodings.Add(EsgyndbEncoding.UTF8, Encoding.UTF8);

            this.ByteOrder = bo; //trigger property update
            this._transport = EsgyndbEncoding.UTF8; //trigger property update
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
                    _encodings[EsgyndbEncoding.UCS2] =  Encoding.GetEncoding(1200);
                }
                else
                {
                    //big endian
                    _encodings[EsgyndbEncoding.UCS2] = Encoding.GetEncoding(1201);
                }
            }
        }

        public EsgyndbEncoding Transport
        {
            get
            {
                return _transport;
            }
        }

        public string GetString(byte[] buf, EsgyndbEncoding c)
        {
            return _encodings[c].GetString(buf);
        }

        public byte[] GetBytes(string s, EsgyndbEncoding c)
        {
            if (s == null)
            {
                return new byte[0];
            }

            return _encodings[c].GetBytes(s);
        }
    }
}
