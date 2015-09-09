﻿
namespace Esgyndb.Data
{
    using System;
    using System.Text.RegularExpressions;
    using System.Collections.Generic;
    using System.Data;

    /// <summary>
    /// Collection of static regular expressions, arrays, and methods used through out the provider.
    /// </summary>
    internal class EsgyndbUtility
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
        /// <returns>A SQL string with Esgyndb compatible delimiters.</returns>
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
         * The following section contains all the type mappings between Esgyndb's custom types and ADO.NET
         *  standard types.
         */ 

        private static readonly Dictionary<DbType, EsgyndbType> DbTypeMapping;
        private static readonly Dictionary<EsgyndbType, DbType> EsgyndbDbTypeMapping;

        static EsgyndbUtility()
        {
            //DbType to EsgyndbType
            DbTypeMapping = new Dictionary<DbType, EsgyndbType>(27);
            DbTypeMapping.Add(DbType.AnsiString, EsgyndbType.Varchar);
            DbTypeMapping.Add(DbType.AnsiStringFixedLength, EsgyndbType.Char);
            //DbTypeMapping.Add(DbType.Binary, EsgyndbType.Undefined);
            //DbTypeMapping.Add(DbType.Boolean, EsgyndbType.Undefined);
            //DbTypeMapping.Add(DbType.Byte, EsgyndbType.Undefined);
            //DbTypeMapping.Add(DbType.Currency, EsgyndbType.Undefined);
            DbTypeMapping.Add(DbType.Date, EsgyndbType.Date);
            DbTypeMapping.Add(DbType.DateTime, EsgyndbType.Timestamp);
            //DbTypeMapping.Add(DbType.DateTime2, EsgyndbType.Undefined);
            //DbTypeMapping.Add(DbType.DateTimeOffset, EsgyndbType.Undefined);
            DbTypeMapping.Add(DbType.Decimal, EsgyndbType.Decimal);
            DbTypeMapping.Add(DbType.Double, EsgyndbType.Double);
            //DbTypeMapping.Add(DbType.Guid, EsgyndbType.Undefined);
            DbTypeMapping.Add(DbType.Int16, EsgyndbType.SmallInt);
            DbTypeMapping.Add(DbType.Int32, EsgyndbType.Integer);
            DbTypeMapping.Add(DbType.Int64, EsgyndbType.LargeInt);
            //DbTypeMapping.Add(DbType.Object, EsgyndbType.Undefined);
            //DbTypeMapping.Add(DbType.SByte, EsgyndbType.Undefined);
            DbTypeMapping.Add(DbType.Single, EsgyndbType.Float);
            DbTypeMapping.Add(DbType.String, EsgyndbType.Varchar); // wrong
            DbTypeMapping.Add(DbType.StringFixedLength, EsgyndbType.Char); // wrong
            DbTypeMapping.Add(DbType.Time, EsgyndbType.Time);
            DbTypeMapping.Add(DbType.UInt16, EsgyndbType.SmallIntUnsigned);
            DbTypeMapping.Add(DbType.UInt32, EsgyndbType.IntegerUnsigned);
            //DbTypeMapping.Add(DbType.UInt64, EsgyndbType.Undefined);
            DbTypeMapping.Add(DbType.VarNumeric, EsgyndbType.Numeric);
            //DbTypeMapping.Add(DbType.Xml, EsgyndbType.Undefined);

            // EsgyndbType to DbType
            EsgyndbDbTypeMapping = new Dictionary<EsgyndbType,DbType>(100);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Undefined, DbType.Object);

            EsgyndbDbTypeMapping.Add(EsgyndbType.Char, DbType.StringFixedLength); //wrong
            EsgyndbDbTypeMapping.Add(EsgyndbType.Date, DbType.Date);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Decimal, DbType.Decimal);
            EsgyndbDbTypeMapping.Add(EsgyndbType.DecimalUnsigned, DbType.Decimal); // wrong
            EsgyndbDbTypeMapping.Add(EsgyndbType.Double, DbType.Double);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Float, DbType.Single);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Integer, DbType.Int32);
            EsgyndbDbTypeMapping.Add(EsgyndbType.IntegerUnsigned, DbType.UInt32);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Interval, DbType.Object); // wrong
            EsgyndbDbTypeMapping.Add(EsgyndbType.LargeInt, DbType.Int64);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Numeric, DbType.VarNumeric);
            EsgyndbDbTypeMapping.Add(EsgyndbType.NumericUnsigned, DbType.VarNumeric); // wrong
            EsgyndbDbTypeMapping.Add(EsgyndbType.Real, DbType.Single);
            EsgyndbDbTypeMapping.Add(EsgyndbType.SmallInt, DbType.Int16);
            EsgyndbDbTypeMapping.Add(EsgyndbType.SmallIntUnsigned, DbType.UInt16);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Time, DbType.Time);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Timestamp, DbType.DateTime);
            EsgyndbDbTypeMapping.Add(EsgyndbType.Varchar, DbType.String); //wrong
        }

        public static EsgyndbType MapDbType(DbType type)
        {
            return DbTypeMapping.ContainsKey(type) ? DbTypeMapping[type] : EsgyndbType.Undefined;
        }

        public static DbType MapEsgyndbDbType(EsgyndbType type)
        {
            return EsgyndbDbTypeMapping.ContainsKey(type) ? EsgyndbDbTypeMapping[type] : DbType.Object;
        }
    }
}