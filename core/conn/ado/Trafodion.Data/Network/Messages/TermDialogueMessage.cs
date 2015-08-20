using System;

namespace Trafodion.Data
{
    /// <summary>
    /// Closes the connection to the server.
    /// </summary>
    internal class TermDialogueMessage: INetworkMessage
    {
        public int DialogueId;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt32(DialogueId);
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            return 4;
        }
    }
}
