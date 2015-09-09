namespace Esgyndb.Data.ETL
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

        public int PrepareMessageParams(EsgyndbEncoder enc)
        {
            return 8;
        }
    }
}