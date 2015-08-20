using System;

namespace Trafodion.Data
{
    internal enum CloseResourceOption: short 
    {
        Close = 0,
        Free = 1
    }

    internal class CloseMessage: INetworkMessage
    {
        public int DialogueId;
        public string Label;
        public CloseResourceOption Option;

        private byte[] _label;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
            ds.WriteString(_label);
            ds.WriteInt16((short)Option);
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            int len = 10; //1*4 Int32, 1*2 Int16, 1*4 string len

            _label = enc.GetBytes(Label, enc.Transport);
            if (_label.Length > 0)
            {
                len += _label.Length + 1;
            }

            return len;
        }
    }
}
