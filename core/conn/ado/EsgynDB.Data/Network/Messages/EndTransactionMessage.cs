using System;

namespace EsgynDB.Data
{
    internal enum EndTransactionOption : short
    {
        Commit = 0,
	    Rollback = 1
    }

    internal class EndTransactionMessage: INetworkMessage
    {
        public int DialogueId;
        public EndTransactionOption Option;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
            ds.WriteInt16((short)Option);
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            return 6; //1*4 Int32, 1*4 Int16
        }
    }
}
