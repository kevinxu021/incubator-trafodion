using System;

namespace Esgyndb.Data
{
    /// <summary>
    /// Represents a message returned by the SQL engine.
    /// </summary>
    internal class SqlWarningOrError: INetworkReply
    {
        public int rowId;
	    public int sqlCode;
	    public string text;
	    public string sqlState;

	    public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc) 
        {
            rowId = ds.ReadInt32();
            sqlCode = ds.ReadInt32();
            text = enc.GetString(ds.ReadString(), enc.Transport);
            sqlState = enc.GetString(ds.ReadBytes(5), enc.Transport);
            ds.ReadByte(); // null term
	    }

        public static SqlWarningOrError [] ReadListFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            int totalErrorLength = ds.ReadInt32();
            SqlWarningOrError[] errorList;

            if (totalErrorLength > 0)
            {
                errorList = new SqlWarningOrError[ds.ReadInt32()];
                for (int i = 0; i < errorList.Length; i++)
                {
                    errorList[i] = new SqlWarningOrError();
                    errorList[i].ReadFromDataStream(ds, enc);
                }
            }
            else
            {
                errorList = new SqlWarningOrError[0];
            }

            return errorList;
        }
    }
}
