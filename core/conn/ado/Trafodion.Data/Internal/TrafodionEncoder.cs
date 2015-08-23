using System.Collections.Generic;
using System.Text;

namespace Trafodion.Data
{
    internal class TrafDbEncoder
    {
        private TrafDbEncoding _transport;
        private ByteOrder _byteOrder;
        private Dictionary<TrafDbEncoding, Encoding> _encodings;

        public TrafDbEncoder(ByteOrder bo)
        {
            _encodings = new Dictionary<TrafDbEncoding, Encoding>(5);
            _encodings.Add(TrafDbEncoding.Default, Encoding.Default);          
            _encodings.Add(TrafDbEncoding.ISO88591, Encoding.GetEncoding("ISO8859-1"));
            _encodings.Add(TrafDbEncoding.UCS2, Encoding.BigEndianUnicode);
            _encodings.Add(TrafDbEncoding.MS932, Encoding.GetEncoding(932));
            _encodings.Add(TrafDbEncoding.UTF8, Encoding.UTF8);

            this.ByteOrder = bo; //trigger property update
            this._transport = TrafDbEncoding.UTF8; //trigger property update
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
                    _encodings[TrafDbEncoding.UCS2] =  Encoding.GetEncoding(1200);
                }
                else
                {
                    //big endian
                    _encodings[TrafDbEncoding.UCS2] = Encoding.GetEncoding(1201);
                }
            }
        }

        public TrafDbEncoding Transport
        {
            get
            {
                return _transport;
            }
        }

        public string GetString(byte[] buf, TrafDbEncoding c)
        {
            return _encodings[c].GetString(buf);
        }

        public byte[] GetBytes(string s, TrafDbEncoding c)
        {
            if (s == null)
            {
                return new byte[0];
            }

            return _encodings[c].GetBytes(s);
        }
    }
}
