using System;

namespace EsgynDB.Data
{
    internal enum EndTransactionError: int
    {
        Success = 0,
        ParamError= 1,
        InvalidConnection = 2,
        SqlError = 3,
        InvalidHandle = 4,
        TransactionError = 5
    }

    internal class EndTransactionReply: INetworkReply
    {
        public EndTransactionError error;
        public int errorDetail;
        public ErrorDesc [] errorDesc;
        public string errorText;

        public void ReadFromDataStream(DataStream ds, EsgynDBEncoder enc)
        {
            error = (EndTransactionError)ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            switch(error) 
            {
                case EndTransactionError.Success:
                case EndTransactionError.InvalidConnection:
                case EndTransactionError.InvalidHandle:
                case EndTransactionError.TransactionError:
                    break;
                case EndTransactionError.ParamError:
                    errorText = enc.GetString(ds.ReadString(), enc.Transport);
                    break;
                case EndTransactionError.SqlError:
                    errorDesc = ErrorDesc.ReadListFromDataStream(ds, enc);
                    break;
            }
        }
    }
}
