using System;

namespace Esgyndb.Data
{
    internal class ExecuteMessage: INetworkMessage
    {
        public int DialogueId;
        public int AsyncEnable;
        public int QueryTimeout;
        public int InputRowCount;
        public int MaxRowsetSize;
        public StatementType SqlStmtType;
        public int StmtHandle;
        public int StmtType;
        public string SqlText;
        public int SqlTextCharset;
        public string CursorName;
        public int CursorNameCharset;
        public string Label;
        public int LabelCharset;
        public string ExplainLabel;
        public byte[] Data;
        public byte[] TransactionId;

        private byte[] _sqlText;
        private byte[] _cursorName;
        private byte[] _label;
        private byte[] _explainLabel;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
		    ds.WriteInt32(AsyncEnable);
		    ds.WriteInt32(QueryTimeout);
		    ds.WriteInt32(InputRowCount);
		    ds.WriteInt32(MaxRowsetSize);
		    ds.WriteInt32((int)SqlStmtType);
		    ds.WriteInt32(StmtHandle);
		    ds.WriteInt32(StmtType);
		    ds.WriteStringWithCharset(_sqlText, SqlTextCharset);
		    ds.WriteStringWithCharset(_cursorName, CursorNameCharset);
		    ds.WriteStringWithCharset(_label, LabelCharset);
		    ds.WriteString(_explainLabel);
            ds.WriteInt32(Data.Length);
            ds.WriteBytes(Data);
			ds.WriteString(TransactionId);
        }

        public int PrepareMessageParams(EsgyndbEncoder enc)
        {
            int len = 56; //9*4 Int32, 5*4 string len

            _sqlText = enc.GetBytes(SqlText, enc.Transport);
            if (_sqlText.Length > 0)
            {
                len += _sqlText.Length + 5; //null + charset
            }

            _cursorName = enc.GetBytes(CursorName, enc.Transport);
            if (_cursorName.Length > 0)
            {
                len += _cursorName.Length + 5; //null + charset
            }

            _label = enc.GetBytes(Label, enc.Transport);
            if (_label.Length > 0)
            {
                len += _label.Length + 5; //null + charset
            }

            _explainLabel = enc.GetBytes(ExplainLabel, enc.Transport);
            if (_explainLabel.Length > 0)
            {
                len += _explainLabel.Length + 1;
            }

            if (Data.Length > 0)
            {
                len += Data.Length;
            }
            if (TransactionId.Length > 0)
            {
                len += TransactionId.Length + 1;
            }

            return len;
        }
    }
}
