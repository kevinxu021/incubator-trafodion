using System;

namespace Esgyndb.Data
{
    internal enum InitDialogueError : int
    {
        Success = 0,
        ParamError = 1,
	    InvalidConnection = 2,
	    SQLError = 3,
	    SQLInvalidHandle = 4,
	    SQLNeedData = 5,
	    InvalidUser = 6
    }

    //static final int SQL_PASSWORD_EXPIRING = 8857;
	//static final int SQL_PASSWORD_GRACEPERIOD = 8837;

    internal class InitDialogueReply: INetworkReply
    {
        public InitDialogueError error;
        public int errorDetail;
        public string errorText;

        public OutConnectionContext outConnectionContext;

        public ErrorDesc [] errorDesc;

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            error = (InitDialogueError)ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            outConnectionContext = new OutConnectionContext();

            switch (error)
            {
                case InitDialogueError.Success:
                    outConnectionContext.ReadFromDataStream(ds, enc);
                    break;
                case InitDialogueError.InvalidUser:
                    //TODO: need to check detail for expiring/grace
                    errorDesc = ErrorDesc.ReadListFromDataStream(ds, enc);
                    outConnectionContext.ReadFromDataStream(ds, enc);
                    break;
                case InitDialogueError.SQLError:
                    errorDesc = ErrorDesc.ReadListFromDataStream(ds, enc);
                    outConnectionContext.ReadFromDataStream(ds, enc);
                    break;
                case InitDialogueError.ParamError:
                    errorText = enc.GetString(ds.ReadString(), enc.Transport);
                    break;
                //TODO: shouldnt there be more here?  need to look up in the server which others return text
            }
        }
    }
}
