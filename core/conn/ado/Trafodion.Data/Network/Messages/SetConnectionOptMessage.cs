using System;

namespace Trafodion.Data
{
    /// <summary>
    /// Updates the value of a connection property on the server.
    /// </summary>
    internal class SetConnectionOptMessage: INetworkMessage
    {
        public int DialogueId;
        public ConnectionOption Option;
        public int IntValue;
        public string StringValue;

        private byte[] _stringValue;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
            ds.WriteInt16((short)Option);
            ds.WriteInt32(IntValue);
            ds.WriteString(_stringValue);
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            int len = 14; // 2*4 Int32, 1*2 Int16, 1*4 string len

            _stringValue = enc.GetBytes(StringValue, enc.Transport);
            if (_stringValue.Length > 0)
            {
                len += _stringValue.Length + 1;
            }

            return len;
        }
    }
}
