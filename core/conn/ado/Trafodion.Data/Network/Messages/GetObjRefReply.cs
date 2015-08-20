using System;

namespace Trafodion.Data
{
    internal enum GetObjRefError: int
    {
        Success = 0,
        ASParamError = 1,
	    ASTimeout = 2,
	    ASNoSrvrHdl = 3,
	    ASTryAgain = 4,
	    ASNotAvailable = 5,
	    DSNotAvailable = 6,
	    PortNotAvailable = 7,
	    InvalidUser = 8,
	    LogonUserFailure = 9,
        Unknown1 = -27, //check on these last two -- why are they here?
        Unknown2 = -29
    }

    internal class GetObjRefReply: INetworkReply
    {
        public GetObjRefError error;
        public int errorDetail;
        public string errorText;

        public string serverObjRef;
        public int dialogueId;
        public string datasource;
        public byte[] userSid;
        public Version [] serverVersion;
        public Boolean securityEnabled;
        public int serverNode;
        public int processId;
        public byte[] timestamp;
        public string cluster;

        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            try
            {
                String serverHostName = "";
                int serverNodeId = 0;
                int serverProcessId = 0;
                String serverProcessName = "";
                String serverIpAddress = "";
                int serverPort = 0;

                error = (GetObjRefError)ds.ReadInt32();
                errorDetail = ds.ReadInt32();
                errorText = enc.GetString(ds.ReadString(), enc.Transport);

                if (error == GetObjRefError.Success)
                {
                    //serverObjRef = enc.GetString(ds.ReadString(), enc.Transport);
                    //serverObjRef = enc.deco(ds.ReadString())
                    dialogueId = ds.ReadInt32();
                    datasource = enc.GetString(ds.ReadString(), enc.Transport);
                    userSid = ds.ReadStringBytes();

                    //version list.
                    serverVersion = Version.ReadListFromDataStream(ds, enc);

                    //old iso mapping
                    // read in isoMapping, not stored anywhere
                    if ((serverVersion[0].BuildId & BuildOptions.Charset) > 0)
                        ds.ReadInt32();

                    serverHostName = enc.GetString(ds.ReadString(), enc.Transport);
                    serverNodeId = ds.ReadInt32();
                    serverProcessId = ds.ReadInt32();
                    serverProcessName = enc.GetString(ds.ReadString(), enc.Transport);
                    serverIpAddress = enc.GetString(ds.ReadString(), enc.Transport);
                    serverPort = ds.ReadInt32();

                    serverObjRef = String.Format("TCP:{0}:{1}.{2},{3}/{4}:ODBC", serverHostName, serverNodeId, serverProcessName, serverIpAddress, serverPort);

                    Console.WriteLine(serverObjRef);

                    if ((serverVersion[0].BuildId & BuildOptions.PasswordSecurity) > 0)
                    {
                        securityEnabled = true;

                        serverNode = serverNodeId;
                        processId = serverProcessId;
                        timestamp = ds.ReadBytes(8);
                        cluster = enc.GetString(ds.ReadString(), enc.Transport);
                    }
                    else
                    {
                        securityEnabled = false;
                    }
                }
            }
            catch (Exception ex)
            {
                String val = ex.Message;
            }
        }
    }
}
