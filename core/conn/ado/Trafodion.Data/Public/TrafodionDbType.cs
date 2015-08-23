namespace Trafodion.Data
{
    using System;

    /// <summary>
    /// Specifies TrafDb-specific data type.
    /// </summary>
    public enum TrafDbDbType
    {
        /// <summary>
        /// Unmappable or unknown datatype.
        /// </summary>
        Undefined = -1,

        /// <summary>
        /// Fixed-length character data.
        /// </summary>
        Char = 1,

        /// <summary>
        /// Variable-length character data.
        /// </summary>
        Varchar = 2,

        /// <summary>
        /// Date in the form yyyy-MM-dd
        /// </summary>
        Date = 3,

        /// <summary>
        /// Time in the form hh:mm:ss[.ffffff]
        /// </summary>
        Time = 4,

        /// <summary>
        /// Timestamp in the form yyyy-MM-dd hh:mm:ss[.ffffff]
        /// </summary>
        Timestamp = 5,

        /// <summary>
        /// Datetime range.
        /// </summary>
        Interval = 6,

        /// <summary>
        /// Signed numeric stored in binary form.
        /// </summary>
        Numeric = 7,

        /// <summary>
        /// Unsigned numeric stored in binary form.
        /// </summary>
        NumericUnsigned = 8,

        /// <summary>
        /// 16 bit signed integer.
        /// </summary>
        SmallInt = 9,

        /// <summary>
        /// 16 bit unsigned integer.
        /// </summary>
        SmallIntUnsigned = 10,

        /// <summary>
        /// 32 bit signed integer.
        /// </summary>
        Integer = 11,

        /// <summary>
        /// 32 bit unsigned integer.
        /// </summary>
        IntegerUnsigned = 12,

        /// <summary>
        /// 64 bit signed integer.
        /// </summary>
        LargeInt = 13,

        /// <summary>
        /// Signed numeric stored in character form.
        /// </summary>
        Decimal = 14,

        /// <summary>
        /// Unsigned numeric stored in character form.
        /// </summary>
        DecimalUnsigned = 15,

        /// <summary>
        /// Approximate numeric stored as an IEEE Float.
        /// </summary>
        Float = 16,

        /// <summary>
        /// Approximate numeric stored with 23 bits of precision and 8 bits of exponent.
        /// </summary>
        Real = 17,

        /// <summary>
        /// Approximate numeric stored as an IEEE Double Precision
        /// </summary>
        Double = 18,

        /// <summary>
        /// Fixed-length character data.
        /// </summary>
        NChar = 19,

        /// <summary>
        /// Variable-length character data.
        /// </summary>
        NVarchar = 20
    }

    /// <summary>
    /// Represents a TrafDb-specific string encoding.
    /// </summary>
    public enum TrafDbEncoding : int
    {
        /// <summary>
        /// Pass-through mode.  Current system encoding is used.
        /// </summary>
        Default = 0,

        /// <summary>
        /// ISO8859-1 standard.
        /// </summary>
        ISO88591 = 1,

        /// <summary>
        /// Microsoft Code Page 932.
        /// </summary>
        MS932 = 10,

        /// <summary>
        /// 2 byte Universal Character Set
        /// </summary>
        UCS2 = 11,

        /// <summary>
        /// 8-bit UCS / Unicode Transformation Format
        /// </summary>
        UTF8 = 15
    }
}
