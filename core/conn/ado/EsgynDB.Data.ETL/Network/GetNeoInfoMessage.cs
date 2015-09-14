namespace EsgynDB.Data.ETL
{
    internal class GetNeoInfoMessage: INetworkMessage
    {
        public void WriteToDataStream(DataStream ds)
        {
            // no data
        }

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            return 0;
        }
    }
}