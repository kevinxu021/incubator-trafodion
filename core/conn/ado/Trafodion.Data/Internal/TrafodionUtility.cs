
namespace Trafodion.Data
{
    using System;
    using System.Text.RegularExpressions;
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Collection of static regular expressions, arrays, and methods used through out the provider.
    /// </summary>
    internal class TrafDbUtility
    {
        /// <summary>
        /// Regular expression for validating numeric format.  Used in BigNum conversion.
        /// </summary>
        public static readonly Regex ValidateNumeric = new Regex("^-{0,1}\\d*\\.{0,1}\\d+$", RegexOptions.Compiled);

        /// <summary>
        /// Regular expression for trimming leading 0s.  Used in BigNum conversion.
        /// </summary>
        public static readonly Regex TrimNumericString = new Regex("0*", RegexOptions.Compiled);
        
        /// <summary>
        /// Regular expression for finding wild char characters in metadata parameters.
        /// </summary>
        public static readonly Regex FindWildCards = new Regex("[%_]", RegexOptions.Compiled);

        /// <summary>
        /// Regular expression for stripping out SQL comments.
        /// </summary>
        public static readonly Regex RemoveComments = new Regex("--.*[\r\n]|/\\*(.|[\r\n])*?\\*/", RegexOptions.Compiled);
        
        /// <summary>
        /// Regular expression for finding out particular num in the sql.
        /// </summary>
        public static readonly Regex FindRowSize = new Regex("[0-9]+", RegexOptions.Compiled);

        /// <summary>
        /// Regular expression for checking if the sql is RWRS User Load
        /// </summary>
        public static readonly Regex FindRowwiseUserLoad = new Regex(@"user\s+load", RegexOptions.Compiled);

        /// <summary>
        /// Regular expression for removing string literals.
        /// </summary>
        public static readonly Regex RemoveStringLiterals = new Regex("\"[^\"]*\"|'[^']*'", RegexOptions.Compiled);
        
        /// <summary>
        /// Regular expression for tokenizing words.
        /// </summary>
        public static readonly Regex Tokenize = new Regex("[^a-zA-Z]+", RegexOptions.Compiled);

        /// <summary>
        /// Most commonly used date format.
        /// </summary>
        public const string DateFormat = "yyyy-MM-dd";

        /// <summary>
        /// Commonly used time formats.   Precision 0 through 6.
        /// </summary>
        public static readonly string[] TimeFormat = 
        { 
            "HH:mm:ss", "HH:mm:ss.f", "HH:mm:ss.ff", "HH:mm:ss.fff", 
            "HH:mm:ss.ffff", "HH:mm:ss.fffff", "HH:mm:ss.ffffff" 
        };

        /// <summary>
        /// Commonly used Timestamp formats.  Precision 0 through 6
        /// </summary>
        public static readonly string[] TimestampFormat = 
        { 
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.f", 
            "yyyy-MM-dd HH:mm:ss.ff", "yyyy-MM-dd HH:mm:ss.fff", 
            "yyyy-MM-dd HH:mm:ss.ffff", "yyyy-MM-dd HH:mm:ss.fffff", 
            "yyyy-MM-dd HH:mm:ss.ffffff" 
        };

        /// <summary>
        /// Array of powers of 10 that can be referenced by using the exponent as the offset
        /// </summary>
        public static readonly long[] PowersOfTen = 
        {
            1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 
            1000000000, 10000000000, 100000000000, 1000000000000,
            10000000000000, 100000000000000, 1000000000000000, 10000000000000000,
            100000000000000000, 1000000000000000000
        };

        /*
         * the following section is a hack implemented to work around that Visual Studio 2008 does not use the proper delimiter 
         * with its query builder.  We need to switch  [ ] to " before executing the statement without breaking 
         * string literals or valid SQL syntax
         * 
         * (""[^""]*"")                              -- find text enclosed in double quotes
         * ('[^']*')                                 -- find text enclosed in single quotes
         * ((\[)(FIRST|ANY|LAST)\s(\d)*?(\]))        -- find text in the form [<keyword> ####], keywords: FIRST, LAST, ANY  
         * 
         */
        
        /// <summary>
        /// Regular expression for removing valid brackets.
        /// </summary>
        public static readonly Regex RemoveValidBrackets = new Regex(@"(""[^""]*"")|('[^']*')|((\[)(FIRST|ANY|LAST)\s(\d)*?(\]))", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Multiline);
        
        /// <summary>
        /// Regular expression for finding all the brackets.
        /// </summary>
        public static readonly Regex FindBrackets = new Regex(@"[\[\]]", RegexOptions.Compiled);

        /// <summary>
        /// Parses a SQL string and replaces [ ] delimiters with " " .
        /// </summary>
        /// <param name="str">The SQL string to be parsed.</param>
        /// <returns>A SQL string with TrafDb compatible delimiters.</returns>
        public static string ConvertBracketIdentifiers(string str)
        {
            // save the raw characters
            char[] chars = str.ToCharArray();

            // replace all the valid brackets and string literals with whitespace equal to the length removed
            str = RemoveValidBrackets.Replace(str, (Match m) => new string(' ', m.Value.Length));

            // find the remaining bracket offsets
            MatchCollection mc = FindBrackets.Matches(str);

            // use the offsets in the temp str to replace the original
            foreach (Match m in mc)
            {
                chars[m.Index] = '"';
            }

            return new string(chars);
        }

        /*
         * The following section contains all the type mappings between TrafDb's custom types and ADO.NET
         *  standard types.
         */ 

        private static readonly Dictionary<DbType, TrafDbDbType> DbTypeMapping;
        private static readonly Dictionary<TrafDbDbType, DbType> TrafDbDbTypeMapping;

        static TrafDbUtility()
        {
            //DbType to TrafDbDbType
            DbTypeMapping = new Dictionary<DbType, TrafDbDbType>(27);
            DbTypeMapping.Add(DbType.AnsiString, TrafDbDbType.Varchar);
            DbTypeMapping.Add(DbType.AnsiStringFixedLength, TrafDbDbType.Char);
            //DbTypeMapping.Add(DbType.Binary, TrafDbDbType.Undefined);
            //DbTypeMapping.Add(DbType.Boolean, TrafDbDbType.Undefined);
            //DbTypeMapping.Add(DbType.Byte, TrafDbDbType.Undefined);
            //DbTypeMapping.Add(DbType.Currency, TrafDbDbType.Undefined);
            DbTypeMapping.Add(DbType.Date, TrafDbDbType.Date);
            DbTypeMapping.Add(DbType.DateTime, TrafDbDbType.Timestamp);
            //DbTypeMapping.Add(DbType.DateTime2, TrafDbDbType.Undefined);
            //DbTypeMapping.Add(DbType.DateTimeOffset, TrafDbDbType.Undefined);
            DbTypeMapping.Add(DbType.Decimal, TrafDbDbType.Decimal);
            DbTypeMapping.Add(DbType.Double, TrafDbDbType.Double);
            //DbTypeMapping.Add(DbType.Guid, TrafDbDbType.Undefined);
            DbTypeMapping.Add(DbType.Int16, TrafDbDbType.SmallInt);
            DbTypeMapping.Add(DbType.Int32, TrafDbDbType.Integer);
            DbTypeMapping.Add(DbType.Int64, TrafDbDbType.LargeInt);
            //DbTypeMapping.Add(DbType.Object, TrafDbDbType.Undefined);
            //DbTypeMapping.Add(DbType.SByte, TrafDbDbType.Undefined);
            DbTypeMapping.Add(DbType.Single, TrafDbDbType.Float);
            DbTypeMapping.Add(DbType.String, TrafDbDbType.Varchar); // wrong
            DbTypeMapping.Add(DbType.StringFixedLength, TrafDbDbType.Char); // wrong
            DbTypeMapping.Add(DbType.Time, TrafDbDbType.Time);
            DbTypeMapping.Add(DbType.UInt16, TrafDbDbType.SmallIntUnsigned);
            DbTypeMapping.Add(DbType.UInt32, TrafDbDbType.IntegerUnsigned);
            //DbTypeMapping.Add(DbType.UInt64, TrafDbDbType.Undefined);
            DbTypeMapping.Add(DbType.VarNumeric, TrafDbDbType.Numeric);
            //DbTypeMapping.Add(DbType.Xml, TrafDbDbType.Undefined);

            // TrafDbDbType to DbType
            TrafDbDbTypeMapping = new Dictionary<TrafDbDbType,DbType>(100);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Undefined, DbType.Object);

            TrafDbDbTypeMapping.Add(TrafDbDbType.Char, DbType.StringFixedLength); //wrong
            TrafDbDbTypeMapping.Add(TrafDbDbType.Date, DbType.Date);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Decimal, DbType.Decimal);
            TrafDbDbTypeMapping.Add(TrafDbDbType.DecimalUnsigned, DbType.Decimal); // wrong
            TrafDbDbTypeMapping.Add(TrafDbDbType.Double, DbType.Double);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Float, DbType.Single);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Integer, DbType.Int32);
            TrafDbDbTypeMapping.Add(TrafDbDbType.IntegerUnsigned, DbType.UInt32);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Interval, DbType.Object); // wrong
            TrafDbDbTypeMapping.Add(TrafDbDbType.LargeInt, DbType.Int64);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Numeric, DbType.VarNumeric);
            TrafDbDbTypeMapping.Add(TrafDbDbType.NumericUnsigned, DbType.VarNumeric); // wrong
            TrafDbDbTypeMapping.Add(TrafDbDbType.Real, DbType.Single);
            TrafDbDbTypeMapping.Add(TrafDbDbType.SmallInt, DbType.Int16);
            TrafDbDbTypeMapping.Add(TrafDbDbType.SmallIntUnsigned, DbType.UInt16);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Time, DbType.Time);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Timestamp, DbType.DateTime);
            TrafDbDbTypeMapping.Add(TrafDbDbType.Varchar, DbType.String); //wrong
        }

        public static TrafDbDbType MapDbType(DbType type)
        {
            return DbTypeMapping.ContainsKey(type) ? DbTypeMapping[type] : TrafDbDbType.Undefined;
        }

        public static DbType MapTrafDbDbType(TrafDbDbType type)
        {
            return TrafDbDbTypeMapping.ContainsKey(type) ? TrafDbDbTypeMapping[type] : DbType.Object;
        }
    }
}