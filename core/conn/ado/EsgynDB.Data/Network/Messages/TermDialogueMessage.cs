using System;

namespace EsgynDB.Data
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

        public int PrepareMessageParams(EsgynDBEncoder enc)
        {
            return 4;
        }
    }
}
