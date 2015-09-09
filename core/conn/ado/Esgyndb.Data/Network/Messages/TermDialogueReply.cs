using System;

namespace Esgyndb.Data
{
    internal enum TermDialogueError : int
    {
        Success = 0,
        ParamError = 1,
	    InvalidConnection = 2,
	    SQLError = 3
    }

    internal class TermDialogueReply: INetworkReply
    {
        public TermDialogueError error;
        public int errorDetail;
        //TODO add SQL error / transaction error

        public void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc)
        {
            error = (TermDialogueError) ds.ReadInt32();
            errorDetail = ds.ReadInt32();

            //add SQLerror
        }
    }
}
