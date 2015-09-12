namespace EsgynDB.Data
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
        internal EsgynDBEncoding SqlEncoding;
        internal EsgynDBEncoding NdcsEncoding;
        internal string ColumnName;
        internal string TableName;
        internal string CatalogName;
        internal string SchemaName;
        internal string HeadingName;
        internal int IntLeadPrec;
        internal ParameterDirection Direction;

        private EsgynDBType EsgyndbDbType;
        private DbType _dbType;

        internal EsgynDBType EsgyndbDataType
        {
            get
            {
                return this.EsgyndbDbType;
            }

            set
            {
                this.EsgyndbDbType = value;
                this._dbType = EsgynDBUtility.MapEsgyndbDbType(this.EsgyndbDbType);
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
                this.EsgyndbDbType = EsgynDBUtility.MapDbType(this._dbType);
            }
        }

        /// <summary>
        /// Reads structured data out of the DataStream
        /// </summary>
        /// <param name="ds">The DataStream object.</param>
        /// <param name="enc">The EsgynDBEncoder object.</param>
        public void ReadFromDataStream(DataStream ds, EsgynDBEncoder enc)
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
            this.SqlEncoding = (EsgynDBEncoding)ds.ReadInt32();
            this.NdcsEncoding = (EsgynDBEncoding)ds.ReadInt32();
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

        internal static Descriptor[] ReadListFromDataStream(DataStream ds, EsgynDBEncoder enc)
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
        /// Maps the descriptor to a DbType and EsgynDBType
        /// </summary>
        private void MapFileSystemType()
        {
            switch (this.FsType)
            {
                case FileSystemType.Varchar:
                case FileSystemType.VarcharDblByte:
                case FileSystemType.VarcharLong:
                case FileSystemType.VarcharWithLength:
                    this.EsgyndbDataType = EsgynDBType.Varchar;
                    break;
                case FileSystemType.Char:
                case FileSystemType.CharDblByte:
                    this.EsgyndbDataType = EsgynDBType.Char;
                    break;
                case FileSystemType.Numeric:
                    this.EsgyndbDataType = EsgynDBType.Numeric;
                    break;
                case FileSystemType.NumericUnsigned:
                    this.EsgyndbDataType = EsgynDBType.NumericUnsigned;
                    break;
                case FileSystemType.Decimal:
                case FileSystemType.DecimalLarge:
                    this.EsgyndbDataType = EsgynDBType.Decimal;
                    break;
                case FileSystemType.DecimalUnsigned:
                case FileSystemType.DecimalLargeUnsigned:
                    this.EsgyndbDataType = EsgynDBType.DecimalUnsigned;
                    break;
                case FileSystemType.Integer:
                    this.EsgyndbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? EsgynDBType.Numeric : EsgynDBType.Integer;
                    break;
                case FileSystemType.IntegerUnsigned:
                    this.EsgyndbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? EsgynDBType.NumericUnsigned : EsgynDBType.IntegerUnsigned;
                    break;
                case FileSystemType.LargeInt:
                    this.EsgyndbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? EsgynDBType.Numeric : EsgynDBType.LargeInt;
                    break;
                case FileSystemType.SmallInt:
                    this.EsgyndbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? EsgynDBType.Numeric : EsgynDBType.SmallInt;
                    break;
                case FileSystemType.SmallIntUnsigned:
                    this.EsgyndbDataType = (this.NdcsDataType == FileSystemType.Numeric) ? EsgynDBType.NumericUnsigned : EsgynDBType.SmallIntUnsigned;
                    break;
                case FileSystemType.Float:
                    this.EsgyndbDataType = EsgynDBType.Float;
                    break;
                case FileSystemType.Real:
                    this.EsgyndbDataType = EsgynDBType.Real;
                    break;
                case FileSystemType.Double:
                    this.EsgyndbDataType = EsgynDBType.Double;
                    break;
                case FileSystemType.DateTime:
                    switch (this.DtCode)
                    {
                        case DateTimeCode.Date:
                            this.EsgyndbDataType = EsgynDBType.Date;
                            break;
                        case DateTimeCode.Time:
                            this.EsgyndbDataType = EsgynDBType.Time;
                            break;
                        case DateTimeCode.Timestamp:
                            this.EsgyndbDataType = EsgynDBType.Timestamp;
                            break;
                        default:
                            throw new Exception("internal error, unknown datetimecode");
                    }

                    break;
                case FileSystemType.Interval:
                    this.EsgyndbDataType = EsgynDBType.Interval;
                    break;
                default:
                    throw new Exception("internal error, unknown fstype");
            }
        }
    }
}
