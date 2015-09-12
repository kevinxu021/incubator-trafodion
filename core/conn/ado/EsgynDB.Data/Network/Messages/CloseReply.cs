using System;

namespace EsgynDB.Data
{
    internal enum CloseError : int
    {
        Success = 0,
        ParamError = 1,
        InvalidConnection = 2,
        SqlError = 3,
        TransactionError = 4
    }

    internal class CloseReply: INetworkReply
    {
        public CloseError error;
        public int errorDetail;
        public ErrorDesc[] errorDesc;
        public string errorText;

        public void ReadFromDataStream(DataStream ds, EsgynDBEncoder enc)
        {
            error = (CloseError)ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            switch (error)
            {
                case CloseError.Success:
                case CloseError.InvalidConnection:
                    break;
                case CloseError.ParamError:
                    errorText = enc.GetString(ds.ReadString(), enc.Transport);
                    break;
                case CloseError.SqlError:
                    errorDesc = ErrorDesc.ReadListFromDataStream(ds, enc);
                    break;
            }
        }
    }
}
