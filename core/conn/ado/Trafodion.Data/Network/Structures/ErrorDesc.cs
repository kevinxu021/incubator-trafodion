using System;

namespace Trafodion.Data
{
    /// <summary>
    /// Represents an error returned by NDCS.
    /// </summary>
    internal class ErrorDesc: INetworkReply
    {
        public int rowId;
        public int errorDiagnosticId;
        public int sqlCode;
        public String sqlState;
        public String errorText;
        public int operationAbortId;
        public int errorCodeType;
        public String Param1;
        public String Param2;
        public String Param3;
        public String Param4;
        public String Param5;
        public String Param6;
        public String Param7;

        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            rowId = ds.ReadInt32();
            errorDiagnosticId = ds.ReadInt32();
            sqlCode = ds.ReadInt32();
            sqlState = System.Text.ASCIIEncoding.ASCII.GetString(ds.ReadBytes(6)); //TODO: is ASCII ok?  what happened to the NULL?
            errorText = enc.GetString(ds.ReadString(), enc.Transport);
            operationAbortId = ds.ReadInt32();
            errorCodeType = ds.ReadInt32();

            Param1 = enc.GetString(ds.ReadString(), enc.Transport);
            Param2 = enc.GetString(ds.ReadString(), enc.Transport);
            Param3 = enc.GetString(ds.ReadString(), enc.Transport);
            Param4 = enc.GetString(ds.ReadString(), enc.Transport);
            Param5 = enc.GetString(ds.ReadString(), enc.Transport);
            Param6 = enc.GetString(ds.ReadString(), enc.Transport);
            Param7 = enc.GetString(ds.ReadString(), enc.Transport);
        }

        public static ErrorDesc[] ReadListFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            int len = ds.ReadInt32();
            ErrorDesc [] errorDesc = new ErrorDesc[len];
            for (int i = 0; i < len; i++)
            {
                errorDesc[i] = new ErrorDesc();
                errorDesc[i].ReadFromDataStream(ds, enc);
            }

            return errorDesc;
        }
    }
}
