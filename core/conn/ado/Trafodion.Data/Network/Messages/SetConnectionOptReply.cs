using System;

namespace Trafodion.Data
{
    internal enum SetConnectionOptError : int
    {
        Success=0,
        ParamError = 1,
        InvalidConnection = 2,
        SqlError = 3,
        InvalidHandle = 4
    }

    internal class SetConnectionOptReply: INetworkReply
    {
        public SetConnectionOptError error;
        public int errorDetail;
        public ErrorDesc [] errorDesc;
        public string errorText;

        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            error = (SetConnectionOptError)ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            switch (error)
            {
                case SetConnectionOptError.Success:
                case SetConnectionOptError.InvalidConnection:
                case SetConnectionOptError.InvalidHandle:
                    break;
                case SetConnectionOptError.ParamError:
                    errorText = enc.GetString(ds.ReadString(), enc.Transport);
                    break;
                case SetConnectionOptError.SqlError:
                    errorDesc = ErrorDesc.ReadListFromDataStream(ds, enc);
                    break;
            }
        }
    }
}
