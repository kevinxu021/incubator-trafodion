using System;

namespace EsgynDB.Data
{
    internal class FetchMessage: INetworkMessage
    {
        public int DialogueId;
        public int AsyncEnable;
        public int QueryTimeout;
        public int StmtHandle;
        public string Label;
        public int LabelCharset;
        public long MaxRowCount;
        public long MaxRowLength;
        public string CursorName;
        public int CursorNameCharset;
        public string Options;

        private byte[] _label;
        private byte[] _cursorName;
        private byte[] _options;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
            ds.WriteInt32(AsyncEnable);
            ds.WriteInt32(QueryTimeout);
            ds.WriteInt32(StmtHandle);
            ds.WriteStringWithCharset(_label, LabelCharset);
            ds.WriteInt64(MaxRowCount);
            ds.WriteInt64(MaxRowLength);
            ds.WriteStringWithCharset(_cursorName, CursorNameCharset);
            ds.WriteString(_options);
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            int len = 44; //4*4 Int32, 2*8 Int64, 3*4 string len

            _cursorName = enc.GetBytes(CursorName, enc.Transport);
            if (_cursorName.Length > 0)
            {
                len += _cursorName.Length + 4; //charset
            }

            _label = enc.GetBytes(Label, enc.Transport);
            if (_label.Length > 0)
            {
                len += _label.Length + 4; //charset
            }

            _options = enc.GetBytes(Options, enc.Transport);
            if (_options.Length > 0)
            {
                len += _options.Length + 1;
            }

            return len;
        }
    }
}
