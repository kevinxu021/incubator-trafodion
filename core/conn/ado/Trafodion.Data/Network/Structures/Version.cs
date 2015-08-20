using System;

namespace Trafodion.Data
{
    /// <summary>
    /// Represents a component version.
    /// </summary>
    internal class Version: INetworkReply, INetworkMessage
    {
        public short ComponentId;
        public short MajorVersion;
        public short MinorVersion;
        public BuildOptions BuildId;

        public void WriteToDataStream(DataStream ds)
        {
            ds.WriteInt16(ComponentId);
            ds.WriteInt16(MajorVersion);
            ds.WriteInt16(MinorVersion);
            ds.WriteUInt32((uint)BuildId);
        }

        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            ComponentId = ds.ReadInt16();
            MajorVersion = ds.ReadInt16();
            MinorVersion = ds.ReadInt16();
            BuildId = (BuildOptions) ds.ReadUInt32();
        }

        public int PrepareMessageParams(TrafDbEncoder enc)
        {
            return 10; //1*4 Int32, 3*2 Int16
        }

        public static Version [] ReadListFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            int len = ds.ReadInt32();
            Version [] version = new Version[len];

            for (int i = 0; i < len; i++)
            {
                version[i] = new Version();
                version[i].ReadFromDataStream(ds, enc);
            }

            return version;
        }
    }
}
