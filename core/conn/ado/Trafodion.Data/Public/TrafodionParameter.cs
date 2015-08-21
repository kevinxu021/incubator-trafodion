namespace Trafodion.Data
{
    using System;
    using System.Data;
    using System.Data.Common;

    /// <summary>
    /// Represents a parameter to a HPDbCommand and optionally its mapping to DataSet columns. This class cannot be inherited.
    /// </summary>
    public sealed class TrafDbParameter : DbParameter, IDbDataParameter, ICloneable
    {
        private Descriptor _descriptor;

        // these properties are not part of the metadata descriptor and are stored directly in this object
        private string _parameterName;
        private DataRowVersion _dataRowVersion;
        private object _value;

        private bool _verified; // if a param is verified, we have real information from the server, not just whatever the user told us

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class.
        /// </summary>
        public TrafDbParameter()
            : this(string.Empty, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class that uses the parameter name and the data type.
        /// </summary>
        /// <param name="name">The name of the parameter to map.</param>
        /// <param name="dataType">One of the TrafDbDbType values.</param>
        /// <exception cref="ArgumentException">The value supplied in the dataType parameter is an invalid back-end data type.</exception>
        public TrafDbParameter(string name, TrafDbDbType dataType)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Public, name, dataType);
            }

            this._descriptor = new Descriptor();

            // this is to handle that 0 constants as the 2nd param will come to this constructor
            // instead of the (string, object) constructor
            if (dataType.Equals((TrafDbDbType)0))
            {
                this._value = 0;
                dataType = TrafDbDbType.Undefined; // make sure we dont get the invalid value exception
            }

            if (!Enum.IsDefined(typeof(TrafDbDbType), dataType))
            {
                string msg = TrafDbResources.FormatMessage(TrafDbMessage.InvalidHPDbDbType, dataType);
                TrafDbException.ThrowException(null, new ArgumentException(msg));
            }

            this.HPDbDbType = dataType;
            this._dataRowVersion = DataRowVersion.Default;
            this._parameterName = name;
            this._verified = false;
        }

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class that uses the parameter name and a value of the new TrafDbParameter.
        /// </summary>
        /// <param name="name">The name of the parameter to map.</param>
        /// <param name="value">An Object that is the value of the TrafDbParameter.</param>
        public TrafDbParameter(string name, object value)
        {
            if (TrafDbTrace.IsPublicEnabled)
            {
                TrafDbTrace.Trace(null, TraceLevel.Public, name, value);
            }

            this._descriptor = new Descriptor() { HPDbDataType = TrafDbDbType.Undefined };
            this._dataRowVersion = DataRowVersion.Default;
            this._parameterName = name;
            this._value = value;
            this._verified = false;
        }

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class that uses the parameter name, the TrafDbDbType, and the size.
        /// </summary>
        /// <param name="name">The name of the parameter to map.</param>
        /// <param name="dataType">One of the TrafDbDbType values. </param>
        /// <param name="size">The length of the parameter.</param>
        /// <exception cref="ArgumentException">The value supplied in the dataType parameter is an invalid back-end data type.</exception>
        public TrafDbParameter(string name, TrafDbDbType dataType, int size)
            : this(name, dataType)
        {
            this.Size = size;
        }

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class that uses the parameter name, the TrafDbDbType, the size, and the source column name.
        /// </summary>
        /// <param name="name">The name of the parameter to map.</param>
        /// <param name="dataType">One of the TrafDbDbType values.</param>
        /// <param name="size">The length of the parameter.</param>
        /// <param name="srcColumn">The name of the source column.</param>
        /// <exception cref="ArgumentException">The value supplied in the dataType parameter is an invalid back-end data type.</exception>
        public TrafDbParameter(string name, TrafDbDbType dataType, int size, string srcColumn)
            : this(name, dataType)
        {
            this.Size = size;
            this.SourceColumn = srcColumn;
            this.Direction = ParameterDirection.Input;
            this.SourceVersion = DataRowVersion.Current;
        }

        /// <summary>
        /// Initializes a new instance of the TrafDbParameter class that uses the parameter name, the type of the parameter, the size of the parameter, a ParameterDirection, the precision of the parameter, the scale of the parameter, the source column, a DataRowVersion to use, and the value of the parameter.
        /// </summary>
        /// <param name="name">The name of the parameter to map.</param>
        /// <param name="dbType">One of the TrafDbDbType values.</param>
        /// <param name="size">The length of the parameter. </param>
        /// <param name="direction">One of the ParameterDirection values. </param>
        /// <param name="isNullable">true if the value of the field can be null; otherwise false.</param>
        /// <param name="precision">The total number of digits to the left and right of the decimal point to which Value is resolved.</param>
        /// <param name="scale">The total number of decimal places to which Value is resolved.</param>
        /// <param name="srcColumn">The name of the source column.</param>
        /// <param name="srcVersion">One of the DataRowVersion values. </param>
        /// <param name="value">An Object that is the value of the TrafDbParameter.</param>
        /// <exception cref="ArgumentException">The value supplied in the dataType parameter is an invalid back-end data type.</exception>
        public TrafDbParameter(
            string name,
            TrafDbDbType dbType,
            int size,
            ParameterDirection direction,
            bool isNullable, 
            byte precision,
            byte scale, 
            string srcColumn,
            DataRowVersion srcVersion,
            object value)
            : this(name, dbType)
        {
            this.Size = size;
            this.Direction = direction;
            this.IsNullable = isNullable;
            this.Precision = precision;
            this.Scale = scale;
            this.SourceColumn = srcColumn;
            this.SourceVersion = srcVersion;

            this._value = value;
        }

        /// <summary>
        /// Gets or sets the DbType of the parameter.
        /// </summary>
        public override DbType DbType
        {
            get
            {
                return this._descriptor.DataType;
            }

            set
            {
                this._verified = false;
                this._descriptor.DataType = value;
            }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the parameter is input-only, output-only, or bidirectional parameter.
        /// </summary>
        public override ParameterDirection Direction
        {
            get
            {
                return this._descriptor.Direction;
            }

            set
            {
                this._verified = false;
                this._descriptor.Direction = value;
            }
        }

        /// <summary>
        /// Gets or sets the encoding of the TrafDbParameter.
        /// </summary>
        [DbProviderSpecificTypePropertyAttribute(true)]
        public HPDbEncoding Encoding
        {
            get
            {
                return this._descriptor.NdcsEncoding;
            }

            set
            {
                this._verified = false;
                this._descriptor.NdcsEncoding = value;
            }
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the parameter accepts null values.
        /// </summary>
        public override bool IsNullable
        {
            get
            {
                return this._descriptor.Nullable;
            }

            set
            {
                this._verified = false;
                this._descriptor.Nullable = value;
            }
        }

        /// <summary>
        /// Gets or sets the TrafDbDbType of the parameter.
        /// </summary>
        [DbProviderSpecificTypePropertyAttribute(true)]
        public TrafDbDbType HPDbDbType
        {
            get
            {
                return this._descriptor.HPDbDataType;
            }

            set
            {
                this._verified = false;
                this._descriptor.HPDbDataType = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the TrafDbParameter.
        /// </summary>
        public override string ParameterName
        {
            get
            {
                return this._parameterName;
            }

            set
            {
                this._parameterName = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum number of digits used to represent the Value property.
        /// </summary>
        [DbProviderSpecificTypePropertyAttribute(true)]
        public int Precision
        {
            get
            {
                return this._descriptor.Precision;
            }

            set
            {
                this._verified = false;
                this._descriptor.Precision = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of decimal places to which Value is resolved.
        /// </summary>
        [DbProviderSpecificTypePropertyAttribute(true)]
        public int Scale
        {
            get
            {
                return this._descriptor.Scale;
            }

            set
            {
                this._verified = false;
                this._descriptor.Scale = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the parameter is signed.
        /// </summary>
        [DbProviderSpecificTypePropertyAttribute(true)]
        public bool Signed
        {
            get
            {
                return this._descriptor.Signed;
            }

            set
            {
                this._verified = false;
                this._descriptor.Signed = value;
            }
        }

        /// <summary>
        /// Gets or sets the maximum size, in bytes, of the data within the column.
        /// </summary>
        public override int Size
        {
            get 
            {
                return this._descriptor.MaxLength;
            }

            set
            {
                this._verified = false;
                this._descriptor.MaxLength = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the source catalog mapped to the DataSet and used for loading or returning the Value
        /// </summary>
        public string SourceCatalog
        {
            get
            {
                return this._descriptor.CatalogName;
            }

            set
            {
                this._verified = false;
                this._descriptor.CatalogName = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the source column mapped to the DataSet and used for loading or returning the Value
        /// </summary>
        public override string SourceColumn
        {
            get
            {
                string val = this._descriptor.ColumnName;

                if (val == null || val.Length == 0)
                {
                    val = this._parameterName;
                }

                return val;
            }

            set
            {
                this._verified = false;
                this._descriptor.ColumnName = value;
            }
        }

        /// <summary>
        /// Always set to false.  SourceColumnNullMapping is disabled for proper CommandBuilder and DataAdapter support.
        /// </summary>
        public override bool SourceColumnNullMapping
        {
            get
            {
                return false;// this.IsNullable;
            }

            set
            {
                //this.IsNullable = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the source column heading mapped to the DataSet and used for loading or returning the Value
        /// </summary>
        public string Heading
        {
            get
            {
                return this._descriptor.HeadingName;
            }

            set
            {
                this._verified = false;
                this._descriptor.HeadingName = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the source schema mapped to the DataSet and used for loading or returning the Value
        /// </summary>
        public string SourceSchema
        {
            get
            {
                return this._descriptor.SchemaName;
            }

            set
            {
                this._verified = false;
                this._descriptor.SchemaName = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the source table mapped to the DataSet and used for loading or returning the Value
        /// </summary>
        public string SourceTable
        {
            get
            {
                return this._descriptor.TableName;
            }

            set
            {
                this._verified = false;
                this._descriptor.TableName = value;
            }
        }

        /// <summary>
        /// Gets or sets the DataRowVersion to use when you load Value
        /// </summary>
        public override DataRowVersion SourceVersion
        {
            get
            {
                return this._dataRowVersion;
            }

            set
            {
                this._dataRowVersion = value;
            }
        }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        public override object Value
        {
            get
            {
                return this._value;
            }

            set
            {
                this._value = value;
            }
        }

        internal Descriptor Descriptor
        {
            get
            {
                return this._descriptor;
            }

            set
            {
                this._descriptor = value;
                this._verified = true;
            }
        }

        internal bool Verified
        {
            get
            {
                return this._verified;
            }
        }

        /// <summary>
        /// Resets the type associated with this TrafDbParameter.
        /// </summary>
        public override void ResetDbType()
        {
            this.ResetHPDbDbType();
        }

        /// <summary>
        /// Resets the type associated with this TrafDbParameter.
        /// </summary>
        public void ResetHPDbDbType()
        {
            this._verified = false;
            this.HPDbDbType = TrafDbDbType.Varchar;
        }

        #region ICloneable

        /// <summary>
        /// Makes a copy of the TrafDbParameter.
        /// </summary>
        /// <returns>A copy of this TrafDbParameter object.</returns>
        object ICloneable.Clone()
        {
            TrafDbParameter clone = new TrafDbParameter(
                this._parameterName, 
                this.HPDbDbType, 
                this.Size, 
                this.Direction,
                this.IsNullable, 
                (byte)this.Precision, 
                (byte)this.Scale, 
                this.SourceColumn, 
                this.SourceVersion, 
                this.Value);
                        
            return clone;
        }

        #endregion
    }
}