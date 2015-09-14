using System;

namespace EsgynDB.Data
{
    internal class ExecuteReply: INetworkReply
    {
        public ReturnCode returnCode;
	    public SqlWarningOrError[] errorList;
	    public long rowsAffected;
	    public QueryType queryType;
	    public int estimatedCost;
	    public byte[] data;

	    public int numResultSets;
	    public Descriptor[][] outputDesc;
	    public string [] stmtLabels;

	    public int rowLength;
	    public int paramCount;

        public void ReadFromDataStream(DataStream ds, EsgynDBEncoder enc)
        {
		    returnCode = (ReturnCode) ds.ReadInt32();

            errorList = SqlWarningOrError.ReadListFromDataStream(ds, enc);

		    int outputDescLength = ds.ReadInt32();
            if (outputDescLength > 0)
            {
                outputDesc = new Descriptor[1][];

                rowLength = ds.ReadInt32();
                outputDesc[0] = Descriptor.ReadListFromDataStream(ds, enc);
            }
            else
            {
                outputDesc = new Descriptor[0][];
            }

		    rowsAffected = ds.ReadUInt32();
		    queryType = (QueryType)ds.ReadInt32();
		    estimatedCost = ds.ReadInt32();

		    // 64 bit rowsAffected
		    // this is a horrible hack because we cannot change the protocol yet
		    // rowsAffected should be made a regular 64 bit value when possible
		    rowsAffected |= ((long) estimatedCost) << 32;

            data = ds.ReadBytes();

		    numResultSets = ds.ReadInt32();

		    if (numResultSets > 0) {
			    outputDesc = new Descriptor[numResultSets][];
			    stmtLabels = new String[numResultSets];

			    for (int i = 0; i < numResultSets; i++) {
				    ds.ReadInt32(); // int stmt_handle

                    stmtLabels[i] = enc.GetString(ds.ReadString(), enc.Transport);

				    ds.ReadInt32(); // long stmt_label_charset
				    outputDescLength = ds.ReadInt32();

                    if (outputDescLength > 0)
                    {
                        paramCount = ds.ReadInt32();
                        outputDesc[i] = Descriptor.ReadListFromDataStream(ds, enc);
                    }
                    else
                    {
                        outputDesc[i] = new Descriptor[0];
                    }

				    ds.ReadString(); //proxy syntax
			    }
		    }

            ds.ReadString(); //single proxy syntax
        }
    }
}
