namespace Esgyndb.Data.ETL
{
    internal class GetNeoInfoMessage: INetworkMessage
    {
        public void WriteToDataStream(DataStream ds)
        {
            // no data
        }

        public int PrepareMessageParams(EsgyndbEncoder enc)
        {
            return 0;
        }
    }
}