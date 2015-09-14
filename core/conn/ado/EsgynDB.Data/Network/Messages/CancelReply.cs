using System;

namespace EsgynDB.Data
{
    internal enum CancelError : int
    {
        Success = 0,
        ParamError = 1,
        ASNotAvailble = 2,
        ServerNotFound = 3,
        ServerInUseByAnotherClient = 4,
        ProcessStopError = 5
    }

    internal class CancelReply : INetworkReply
    {
        public CancelError error;
        public int errorDetail;
        public string errorText;

        public void ReadFromDataStream(DataStream ds, EsgynDBEncoder enc)
        {
            error = (CancelError)ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            if(error != CancelError.Success) 
            {
                errorText = enc.GetString(ds.ReadString(), enc.Transport);
            }
        }
    }
}
