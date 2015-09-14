
namespace EsgynDB.Data
{
    using System;
    using System.Text.RegularExpressions;
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Collection of static regular expressions, arrays, and methods used through out the provider.
    /// </summary>
    internal class EsgynDBUtility
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
        /// <returns>A SQL string with EsgynDB compatible delimiters.</returns>
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
         * The following section contains all the type mappings between EsgynDB's custom types and ADO.NET
         *  standard types.
         */ 

        private static readonly Dictionary<DbType, EsgynDBType> DbTypeMapping;
        private static readonly Dictionary<EsgynDBType, DbType> EsgyndbDbTypeMapping;

        static EsgynDBUtility()
        {
            //DbType to EsgynDBType
            DbTypeMapping = new Dictionary<DbType, EsgynDBType>(27);
            DbTypeMapping.Add(DbType.AnsiString, EsgynDBType.Varchar);
            DbTypeMapping.Add(DbType.AnsiStringFixedLength, EsgynDBType.Char);
            //DbTypeMapping.Add(DbType.Binary, EsgynDBType.Undefined);
            //DbTypeMapping.Add(DbType.Boolean, EsgynDBType.Undefined);
            //DbTypeMapping.Add(DbType.Byte, EsgynDBType.Undefined);
            //DbTypeMapping.Add(DbType.Currency, EsgynDBType.Undefined);
            DbTypeMapping.Add(DbType.Date, EsgynDBType.Date);
            DbTypeMapping.Add(DbType.DateTime, EsgynDBType.Timestamp);
            //DbTypeMapping.Add(DbType.DateTime2, EsgynDBType.Undefined);
            //DbTypeMapping.Add(DbType.DateTimeOffset, EsgynDBType.Undefined);
            DbTypeMapping.Add(DbType.Decimal, EsgynDBType.Decimal);
            DbTypeMapping.Add(DbType.Double, EsgynDBType.Double);
            //DbTypeMapping.Add(DbType.Guid, EsgynDBType.Undefined);
            DbTypeMapping.Add(DbType.Int16, EsgynDBType.SmallInt);
            DbTypeMapping.Add(DbType.Int32, EsgynDBType.Integer);
            DbTypeMapping.Add(DbType.Int64, EsgynDBType.LargeInt);
            //DbTypeMapping.Add(DbType.Object, EsgynDBType.Undefined);
            //DbTypeMapping.Add(DbType.SByte, EsgynDBType.Undefined);
            DbTypeMapping.Add(DbType.Single, EsgynDBType.Float);
            DbTypeMapping.Add(DbType.String, EsgynDBType.Varchar); // wrong
            DbTypeMapping.Add(DbType.StringFixedLength, EsgynDBType.Char); // wrong
            DbTypeMapping.Add(DbType.Time, EsgynDBType.Time);
            DbTypeMapping.Add(DbType.UInt16, EsgynDBType.SmallIntUnsigned);
            DbTypeMapping.Add(DbType.UInt32, EsgynDBType.IntegerUnsigned);
            //DbTypeMapping.Add(DbType.UInt64, EsgynDBType.Undefined);
            DbTypeMapping.Add(DbType.VarNumeric, EsgynDBType.Numeric);
            //DbTypeMapping.Add(DbType.Xml, EsgynDBType.Undefined);

            // EsgynDBType to DbType
            EsgyndbDbTypeMapping = new Dictionary<EsgynDBType,DbType>(100);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Undefined, DbType.Object);

            EsgyndbDbTypeMapping.Add(EsgynDBType.Char, DbType.StringFixedLength); //wrong
            EsgyndbDbTypeMapping.Add(EsgynDBType.Date, DbType.Date);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Decimal, DbType.Decimal);
            EsgyndbDbTypeMapping.Add(EsgynDBType.DecimalUnsigned, DbType.Decimal); // wrong
            EsgyndbDbTypeMapping.Add(EsgynDBType.Double, DbType.Double);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Float, DbType.Single);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Integer, DbType.Int32);
            EsgyndbDbTypeMapping.Add(EsgynDBType.IntegerUnsigned, DbType.UInt32);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Interval, DbType.Object); // wrong
            EsgyndbDbTypeMapping.Add(EsgynDBType.LargeInt, DbType.Int64);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Numeric, DbType.VarNumeric);
            EsgyndbDbTypeMapping.Add(EsgynDBType.NumericUnsigned, DbType.VarNumeric); // wrong
            EsgyndbDbTypeMapping.Add(EsgynDBType.Real, DbType.Single);
            EsgyndbDbTypeMapping.Add(EsgynDBType.SmallInt, DbType.Int16);
            EsgyndbDbTypeMapping.Add(EsgynDBType.SmallIntUnsigned, DbType.UInt16);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Time, DbType.Time);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Timestamp, DbType.DateTime);
            EsgyndbDbTypeMapping.Add(EsgynDBType.Varchar, DbType.String); //wrong
        }

        public static EsgynDBType MapDbType(DbType type)
        {
            return DbTypeMapping.ContainsKey(type) ? DbTypeMapping[type] : EsgynDBType.Undefined;
        }

        public static DbType MapEsgyndbDbType(EsgynDBType type)
        {
            return EsgyndbDbTypeMapping.ContainsKey(type) ? EsgyndbDbTypeMapping[type] : DbType.Object;
        }
    }
}