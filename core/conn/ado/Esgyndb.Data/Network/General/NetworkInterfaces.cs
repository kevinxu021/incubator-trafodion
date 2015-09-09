using System;

namespace Esgyndb.Data
{
    /// <summary>
    /// Common interface for all structures marshalled to the server.
    /// </summary>
    internal interface INetworkMessage
    {
        /// <summary>
        /// Writes structured data into the <code>DataStream</code>
        /// </summary>
        /// <param name="ds"></param>
        void WriteToDataStream(DataStream ds);

        /// <summary>
        /// converts string params to bytes and calculates the total size of the message
        /// </summary>
        /// <returns>the total size for the message</returns>
        int PrepareMessageParams(EsgyndbEncoder enc);
    }

    /// <summary>
    /// Common interface for all structures marshalled from the server.
    /// </summary>
    internal interface INetworkReply
    {
        /// <summary>
        /// Reads structured data out of the DataStream
        /// </summary>
        /// <param name="ds">The DataStream object.</param>
        /// <param name="enc">The EsgyndbEncoder object.</param>
        void ReadFromDataStream(DataStream ds, EsgyndbEncoder enc);
    }
}
