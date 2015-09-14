namespace EsgynDB.Data.ETL
{
    internal class GetParallelExtractInfoMessage: INetworkMessage
    {
        public int ServerStmtHandle;
        public int NumStreams;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(this.ServerStmtHandle);
            ds.WriteInt32(this.NumStreams);
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            return 8;
        }
    }
}