namespace Trafodion.Data
{
    using System;
    using System.Data;

    /// <summary>
    /// Holds the metadata of a parameter or column.
    /// </summary>
    internal class Descriptor : INetworkReply
    {
        internal int DataOffset;
        internal int NullOffset;
        internal int Version;
        internal FileSystemType FsType;
        internal DateTimeCode DtCode;
        internal int MaxLength;
        internal int Precision;
        internal int Scale;
        internal bool Nullable;
        internal bool Signed;
        internal FileSystemType NdcsDataType;
        internal int NdcsPrecision;
        internal HPDbEncoding SqlEncoding;
        internal HPDbEncoding NdcsEncoding;
        internal string ColumnName;
        internal string TableName;
        internal string CatalogName;
        internal string SchemaName;
        internal string HeadingName;
        internal int IntLeadPrec;
        internal ParameterDirection Direction;

        private TrafDbDbType _HPDbDbType;
        private DbType _dbType;

        internal TrafDbDbType HPDbDataType
        {
            get
            {
                return this._HPDbDbType;
            }

            set
            {
                this._HPDbDbType = value;
                this._dbType = TrafDbUtility.MapHPDbDbType(this._HPDbDbType);
            }
        }

        internal DbType DataType
        {
            get
            {
                return this._dbType;
            }

            set
            {
                this._dbType = value;
                this._HPDbDbType = TrafDbUtility.MapDbType(this._dbType);
            }
        }

        /// <summary>
        /// Reads structured data out of the DataStream
        /// </summary>
        /// <param name="ds">The DataStream object.</param>
        /// <param name="enc">The TrafDbEncoder object.</param>
        public void ReadFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            this.DataOffset = ds.ReadInt32();
            this.NullOffset = ds.ReadInt32();
            this.Version = ds.ReadInt32();
            this.FsType = (FileSystemType)ds.ReadInt32();
            this.DtCode = (DateTimeCode)ds.ReadInt32();
            this.MaxLength = ds.ReadInt32();
            this.Precision = ds.ReadInt32();
            this.Scale = ds.ReadInt32();
            this.Nullable = ds.ReadInt32() == 1;
            this.Signed = ds.ReadInt32() == 1;
            this.NdcsDataType = (FileSystemType)ds.ReadInt32();
            this.NdcsPrecision = ds.ReadInt32();
            this.SqlEncoding = (HPDbEncoding)ds.ReadInt32();
            this.NdcsEncoding = (HPDbEncoding)ds.ReadInt32();
            this.ColumnName = enc.GetString(ds.ReadString(), enc.Transport);
            this.TableName = enc.GetString(ds.ReadString(), enc.Transport);
            this.CatalogName = enc.GetString(ds.ReadString(), enc.Transport);
            this.SchemaName = enc.GetString(ds.ReadString(), enc.Transport);
            this.HeadingName = enc.GetString(ds.ReadString(), enc.Transport);
            this.IntLeadPrec = ds.ReadInt32();

            int pm = ds.ReadInt32();
            if (pm == 2)
            {
                this.Direction = ParameterDirection.InputOutput;
            }
            else if (pm == 4)
            {
                this.Direction = ParameterDirection.Output;
            }
            else
            {
                this.Direction = ParameterDirection.Input;
            }

            this.MapFileSystemType();
        }

        internal static Descriptor[] ReadListFromDataStream(DataStream ds, TrafDbEncoder enc)
        {
            int count = ds.ReadInt32();
            Descriptor[] desc = new Descriptor[count];

            for (int i = 0; i < desc.Length; i++)
            {
                desc[i] = new Descriptor();
                desc[i].ReadFromDataStream(ds, enc);
            }

            return desc;
        }

        /// <summary>
        /// Maps the descriptor to a DbType and TrafDbDbType
        /// </summary>
        private void MapFileSystemType()
        {
            switch (this.FsType)
            {
                case FileSystemType.Varchar:
                case FileSystemType.VarcharDblByte:
                case FileSystemType.VarcharLong:
                case FileSystemType.VarcharWithLength:
                    this.HPDbDataType = TrafDbDbType.Varchar;
                    break;
                case FileSystemType.Char:
                case FileSystemType.CharDblByte:
                    this.HPDbDataType = TrafDbDbType.Char;
                    break;
                case FileSystemType.Numeric:
                    this.HPDbDataType = TrafDbDbType.Numeric;
                    break;
                case FileSystemType.NumericUnsigned:
                    this.HPDbDataType = TrafDbDbType.NumericUnsigned;
                    break;
                case FileSystemType.Decimal:
                case FileSystemType.DecimalLarge:
                    this.HPDbDataType = TrafDbDbType.Decimal;
                    break;
                case FileSystemType.DecimalUnsigned:
                case FileSystemType.DecimalLargeUnsigned:
                    this.HPDbDataType = TrafDbDbType.DecimalUnsigned;
                    break;
                case FileSystemType.Integer:
                    this.HPDbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? TrafDbDbType.Numeric : TrafDbDbType.Integer;
                    break;
                case FileSystemType.IntegerUnsigned:
                    this.HPDbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? TrafDbDbType.NumericUnsigned : TrafDbDbType.IntegerUnsigned;
                    break;
                case FileSystemType.LargeInt:
                    this.HPDbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? TrafDbDbType.Numeric : TrafDbDbType.LargeInt;
                    break;
                case FileSystemType.SmallInt:
                    this.HPDbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? TrafDbDbType.Numeric : TrafDbDbType.SmallInt;
                    break;
                case FileSystemType.SmallIntUnsigned:
                    this.HPDbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? TrafDbDbType.NumericUnsigned : TrafDbDbType.SmallIntUnsigned;
                    break;
                case FileSystemType.Float:
                    this.HPDbDataType = TrafDbDbType.Float;
                    break;
                case FileSystemType.Real:
                    this.HPDbDataType = TrafDbDbType.Real;
                    break;
                case FileSystemType.Double:
                    this.HPDbDataType = TrafDbDbType.Double;
                    break;
                case FileSystemType.DateTime:
                    switch (this.DtCode)
                    {
                        case DateTimeCode.Date:
                            this.HPDbDataType = TrafDbDbType.Date;
                            break;
                        case DateTimeCode.Time:
                            this.HPDbDataType = TrafDbDbType.Time;
                            break;
                        case DateTimeCode.Timestamp:
                            this.HPDbDataType = TrafDbDbType.Timestamp;
                            break;
                        default:
                            throw new Exception("internal error, unknown datetimecode");
                    }

                    break;
                case FileSystemType.Interval:
                    this.HPDbDataType = TrafDbDbType.Interval;
                    break;
                default:
                    throw new Exception("internal error, unknown fstype");
            }
        }
    }
}
