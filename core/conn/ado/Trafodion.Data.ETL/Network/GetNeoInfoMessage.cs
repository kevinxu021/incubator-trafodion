namespace Trafodion.Data.ETL
{
    internal class GetNeoInfoMessage: INetworkMessage
    {
        public void WriteToDataStream(DataStream ds)
        {
            // no data
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            return 0;
        }
    }
}