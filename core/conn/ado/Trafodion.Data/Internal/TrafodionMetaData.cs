using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Data.Common;
using System.IO;
using System.Windows.Forms;

namespace Trafodion.Data
{
    /// <summary>
    /// NOTE: you cannot use pattern matching in catalogs besides in the catalog and schema collections
    /// 
    /// Reference: http://msdn.microsoft.com/en-us/library/ms254501.aspx
    /// </summary>
    internal class TrafDbMetaData
    {
        private static string CatalogQuery = "select " +
            "distinct cast ('trafodion' as varchar(128)) CatalogName " +
            "from trafodion.\"_MD_\".OBJECTS;";

        private static string SchemaQuery = "select " +
            "distinct cast ('trafodion' as varchar(128)) CatalogName, " +
            "cast(trim(ob.SCHEMA_NAME) as varchar(128)) SchemaName " +
            "from trafodion.\"_MD_\".OBJECTS ob";

        private static string TableQuery = "select " +
           "distinct cast('{0}' as varchar(128)) CatalogName, " +
           "cast(trim(SCHEMA_NAME) as varchar(128)) SchemaName, " +
           "cast(trim(OBJECT_NAME) as varchar(128)) TableName " +
           "from TRAFODION.\"_MD_\".OBJECTS " +
           "where rtrim(SCHEMA_NAME) {1} translate('{2}' using UTF8TOUCS2) and " +
           "rtrim(OBJECT_NAME) {3} translate('{4}' using UTF8TOUCS2) " +
           "and ((SCHEMA_NAME <> '_MD_') and OBJECT_TYPE in ('BT')) " +
           "for read uncommitted access order by 1, 2, 3;";
        private static string ViewQuery = "select " +
            "distinct cast('TRAFODION' as varchar(128)) CatalogName, " +
            "cast(trim(SCHEMA_NAME) as varchar(128)) SchemaName, " +
            "cast(trim(OBJECT_NAME) as varchar(128)) TableName " +
            "from TRAFODION.\"_MD_\".OBJECTS " +
            "where rtrim(SCHEMA_NAME) {1} translate('{2}' using UTF8TOUCS2) and " +
            "rtrim(OBJECT_NAME) {3} translate('{4}' using UTF8TOUCS2) " +
            "and ((SCHEMA_NAME <> '_MD_') and OBJECT_TYPE in ('VI')) " +
            "for read uncommitted access order by 1, 2, 3;";
        private static string IndexQuery = "select " +
            "distinct cast('TRAFODION' as varchar(128)) CatalogName, " +
            "cast(trim(SCHEMA_NAME) as varchar(128)) SchemaName, " +
            "cast(trim(OBJECT_NAME) as varchar(128)) TableName " +
            "from TRAFODION.\"_MD_\".OBJECTS " +
            "where rtrim(SCHEMA_NAME) {1} translate('{2}' using UTF8TOUCS2) and " +
            "rtrim(OBJECT_NAME) {3} translate('{4}' using UTF8TOUCS2) " +
            "and ((SCHEMA_NAME <> '_MD_') and OBJECT_TYPE in ('IX')) " +
            "for read uncommitted access order by 1, 2, 3;";


        private static string ProcedureQuery = "";

        private static string ColumnQuery = "select " +
            "cast( 'TRAFODION' as varchar(128)) CatalogName, " +
            "cast(trim(ob.SCHEMA_NAME) as varchar(128)) SchemaName, " +
            "cast(trim(ob.OBJECT_NAME) as varchar(128)) TableName, " +
            "cast(trim(co.COLUMN_NAME) as varchar(128)) ColumnName," +
            "cast((case when co.FS_DATA_TYPE = 0 and co.character_set = 'UCS2' then -8 " +
            "when co.FS_DATA_TYPE = 64 and co.character_set = 'UCS2' then -9 else dt.DATA_TYPE end) as smallint) DataType, " +
            "trim(dt.TYPE_NAME) TypeName, " +
            "cast((case when co.FS_DATA_TYPE = 0 and co.character_set = 'UCS2' then co.COLUMN_SIZE/2 " +
            "when co.FS_DATA_TYPE = 64 and co.character_set = 'UCS2' then co.COLUMN_SIZE/2 " +
            "when dt.USEPRECISION = -1 then co.COLUMN_SIZE when dt.USEPRECISION = -2 then co.COLUMN_PRECISION " +
            "when co.FS_DATA_TYPE = 192 then dt.USEPRECISION + 1 " +
            "when co.FS_DATA_TYPE >= 195 and co.FS_DATA_TYPE <= 207 then dt.USEPRECISION + 1 " +
            "else dt.USEPRECISION end) as integer) ColumnSize, " +
            "cast ((case when dt.USELENGTH = -1 then co.COLUMN_SIZE when dt.USELENGTH = -2 then co.COLUMN_PRECISION " +
            "when dt.USELENGTH = -3 then co.COLUMN_PRECISION + 2 " +
            "else dt.USELENGTH end) as integer ) BufferLen, " +
            "cast(co.COLUMN_SCALE as smallint) DecimalDigits, " +
            "cast(dt.NUM_PREC_RADIX as smallint) NumPrecRadix, " +
            "cast((case when co.NULLABLE =0 then 0 when co.NULLABLE = 2 then 1 else 2 end) as smallint) NullAble, " +
            "cast(NULL as varchar(128)) Remarks, " +
            "trim(co.DEFAULT_VALUE) ColumnDef, " +
            "cast((case when co.FS_DATA_TYPE = 0 and co.character_set = 'UCS2' then -8 " +
            "when co.FS_DATA_TYPE = 64 and co.character_set = 'UCS2' then -9 else dt.SQL_DATA_TYPE end) as smallint) SQL_DATA_TYPE, " +
            "cast(dt.SQL_DATETIME_SUB as smallint) SQL_DATETIME_SUB, " +
            "cast((case dt.DATA_TYPE when 1 then co.COLUMN_SIZE " +
            "when -1 then co.COLUMN_SIZE when 12 then co.COLUMN_SIZE else NULL end) as integer) CHAR_OCTET_LENGTH, " +
            "cast((case when (trim(co.COLUMN_CLASS) <> 'S') then co.COLUMN_NUMBER + 1 else " +
            "co.COLUMN_NUMBER end) as integer) ORDINAL_POSITION," +
            "cast((case when co.NULLABLE = 0 then 'NO' else 'YES' end) as varchar(3)) IS_NULLABLE " +
            "from " +
            "TRAFODION.\"_MD_\".objects ob, " +
            "TRAFODION.\"_MD_\".columns co, " +
            "TRAFODION.\"_MD_\".columns co1, " +
            "(VALUES (" +
            "cast('BIGINT' as varchar(128)),cast(-5 as smallint), cast(19 as integer), cast (NULL as varchar(128)), cast (NULL as varchar(128)), " +
            "cast (NULL as varchar(128)), cast(1 as smallint), cast(0 as smallint), cast(2 as smallint) , cast(0 as smallint), cast(0 as smallint), " +
            "cast(0 as smallint), cast('LARGEINT' as varchar(128)), cast(NULL as smallint), cast(NULL as smallint), cast('SIGNED LARGEINT' as varchar(128)), cast(134 as integer), " +
            "cast(10 as smallint), cast(19 as integer), cast(20 as integer), cast(-402 as smallint), cast(NULL as smallint), cast(NULL as smallint), " +
            "cast(0 as smallint), cast(0 as smallint), cast(3 as smallint), cast(0 as smallint)), " +
            "('CHAR', 1, 32000, '''', '''', 'max length', 1, 1, 3, NULL, 0, NULL, 'CHARACTER', NULL, NULL, 'CHARACTER', 0, NULL, -1, -1, 1, NULL, NULL, 0, 0, 3, 0), " +
            "('DATE', 91, 10, '{{d ''', '''}}', NULL, 1, 0, 2, NULL, 0, NULL, 'DATE', NULL, NULL, 'DATE', 192, NULL, 10, 6, 9, 1, NULL, 1, 3, 3, 0), " +
            "('DECIMAL', 3, 18, NULL, NULL, 'precision,scale', 1, 0, 2, 0, 0, 0, 'DECIMAL', 0, 18, 'SIGNED DECIMAL', 152, 10, -2, -3, 3, NULL, NULL, 0, 0, 3, 0), " +
            "('DECIMAL UNSIGNED', 3, 18, NULL, NULL, 'precision,scale', 1, 0, 2, 1, 0, 0, 'DECIMAL', 0, 18, 'UNSIGNED DECIMAL', 150, 10, -2, -3, -301, NULL, NULL, 0, 0, 3, 0), " +
            "('DOUBLE PRECISION', 8, 15, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'DOUBLE', NULL, NULL, 'DOUBLE', 143, 2, 54, -1, 8, NULL, NULL, 0, 0, 3, 0), " +
            "('FLOAT', 6, 15, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'FLOAT', NULL, NULL, 'FLOAT', 142, 2, -2, -1, 6, NULL, NULL, 0, 0, 3, 0), " +
            "('INTEGER', 4, 10, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'INTEGER', NULL, NULL, 'SIGNED INTEGER', 132, 10, 10, -1, 4, NULL, NULL, 0, 0, 3, 0), " +
            "('INTEGER UNSIGNED', 4, 10, NULL, NULL, NULL, 1, 0, 2, 1, 0, 0, 'INTEGER', NULL, NULL, 'UNSIGNED INTEGER', 133, 10, 10, -1, -401, NULL, NULL, 0, 0, 3, 0), " +
            "('INTERVAL', 113, 0, '{{INTERVAL ''', ''' MINUTE TO SECOND}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 207, NULL, 3, 34, 100, 13, 2, 5, 6, 3, 0), " +
            "('INTERVAL', 105, 0, '{{INTERVAL ''', ''' MINUTE}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 199, NULL, 0, 34, 100, 5, 2, 5, 5, 3, 0), " +
            "('INTERVAL', 101, 0, '{{INTERVAL ''', ''' YEAR}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 195, NULL, 0, 34, 100, 1, 2, 1, 1, 3, 0), " +
            "('INTERVAL', 106, 0, '{{INTERVAL ''', ''' SECOND}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 200, NULL, 0, 34, 100, 6, 2, 6, 6, 3, 0), " +
            "('INTERVAL', 104, 0, '{{INTERVAL ''', ''' HOUR}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 198, NULL, 0, 34, 100, 4, 2, 4, 4, 3, 0), " +
            "('INTERVAL', 107, 0, '{{INTERVAL ''', ''' YEAR TO MONTH}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 201, NULL, 3, 34, 100, 7, 2, 1, 2, 3, 0), " +
            "('INTERVAL', 108, 0, '{{INTERVAL ''', ''' DAY TO HOUR}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 202, NULL, 3, 34, 100, 8, 2, 3, 4, 3, 0), " +
            "('INTERVAL', 102, 0, '{{INTERVAL ''', ''' MONTH}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 196, NULL, 0, 34, 100, 2, 2, 2, 2, 3, 0), " +
            "('INTERVAL', 111, 0, '{{ ''', ''' HOUR TO MINUTE}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 205, NULL, 3, 34, 100, 11, 2, 4, 5, 3, 0), " +
            "('INTERVAL', 112, 0, '{{INTERVAL ''', ''' HOUR TO SECOND}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 206, NULL, 6, 34, 100, 12, 2, 4, 6, 3, 0), " +
            "('INTERVAL', 110, 0, '{{INTERVAL ''', ''' DAY TO SECOND}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 204, NULL, 9, 34, 100, 10, 2, 3, 6, 3, 0), " +
            "('INTERVAL', 109, 0, '{{INTERVAL ''', ''' DAY TO MINUTE}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 203, NULL, 6, 34, 100, 9, 2, 3, 5, 3, 0), " +
            "('INTERVAL', 103, 0, '{{INTERVAL ''', ''' DAY}}', NULL, 1, 0, 2, 0, 0, NULL, 'INTERVAL', 0, 0, 'INTERVAL', 197, NULL, 0, 34, 100, 3, 2, 3, 3, 3, 0), " +
            "('NUMERIC', 2, 128, NULL, NULL, 'precision,scale', 1, 0, 2, 0, 0, 0, 'NUMERIC', 0, 128, 'SIGNED NUMERIC', 156, 10, -2, -3, 2, NULL, NULL, 0, 0, 3, 0), " +
            "('NUMERIC UNSIGNED', 2, 128, NULL, NULL, 'precision,scale', 1, 0, 2, 1, 0, 0, 'NUMERIC', 0, 128, 'UNSIGNED NUMERIC', 155, 10, -2, -3, 2, NULL, NULL, 0, 0, 3, 0), " +
            "('REAL', 7, 7, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'REAL', NULL, NULL, 'REAL', 142, 2, 22, -1, 7, NULL, NULL, 0, 0, 3, 0), " +
            "('SMALLINT', 5, 5, NULL, NULL, NULL, 1, 0, 2, 0, 0, 0, 'SMALLINT', NULL, NULL, 'SIGNED SMALLINT', 130, 10, 5, -1, 5, NULL, NULL, 0, 0, 3, 0), " +
            "('SMALLINT UNSIGNED', 5, 5, NULL, NULL, NULL, 1, 0, 2, 1, 0, 0, 'SMALLINT', NULL, NULL, 'UNSIGNED SMALLINT', 131, 10, 5, -1, -502, NULL, NULL, 0, 0, 3, 0), " +
            "('TIME', 92, 8, '{{t ''', '''}}', NULL, 1, 0, 2, NULL, 0, NULL, 'TIME', NULL, NULL, 'TIME', 192, NULL, 8, 6, 9, 2, NULL, 4, 6, 3, 0), " +
            "('TIMESTAMP', 93, 26, '{{ts ''', '''}}', NULL, 1, 0, 2, NULL, 0, NULL, 'TIMESTAMP', 0, 6, 'TIMESTAMP', 192, NULL, 19, 16, 9, 3, NULL, 1, 6, 3, 0), " +
            "('VARCHAR', 12, 32000, '''', '''', 'max length', 1, 1, 3, NULL, 0, NULL, 'VARCHAR', NULL, NULL, 'VARCHAR', 64, NULL, -1, -1, 12, NULL, NULL, 0, 0, 3, 0) " +
            " ) " +
            "dt(\"TYPE_NAME\", \"DATA_TYPE\", \"PREC\", \"LITERAL_PREFIX\", \"LITERAL_SUFFIX\", \"CREATE_PARAMS\", \"IS_NULLABLE\", \"CASE_SENSITIVE\", \"SEARCHABLE\", " +
            "\"UNSIGNED_ATTRIBUTE\", \"FIXED_PREC_SCALE\", \"AUTO_UNIQUE_VALUE\", \"LOCAL_TYPE_NAME\", \"MINIMUM_SCALE\", \"MAXIMUM_SCALE\", \"SQL_TYPE_NAME\", \"FS_DATA_TYPE\", " +
            "\"NUM_PREC_RADIX\", \"USEPRECISION\", \"USELENGTH\", \"SQL_DATA_TYPE\", \"SQL_DATETIME_SUB\", \"INTERVAL_PRECISION\", \"DATETIMESTARTFIELD\", " +
            "\"DATETIMEENDFIELD\", \"APPLICATION_VERSION\", \"TRANSLATION_ID\") " +
            "where ob.OBJECT_UID = co.OBJECT_UID " +
            "and dt.FS_DATA_TYPE = co.FS_DATA_TYPE " +
            "and co.OBJECT_UID = co1.OBJECT_UID and co1.COLUMN_NUMBER = 0 " +
            "and (dt.DATETIMESTARTFIELD = co.DATETIME_START_FIELD) " +
            "and (dt.DATETIMEENDFIELD = co.DATETIME_END_FIELD) " +
            "and ob.SCHEMA_NAME = '{2}' " +
            "and ob.OBJECT_NAME = '{4}' " +
            "and rtrim(ob.SCHEMA_NAME) {1} translate('{2}' using UTF8TOUCS2) " +
            "and rtrim(ob.OBJECT_NAME) {3} translate('{4}' using UTF8TOUCS2) " +
            "and rtrim(co.COLUMN_NAME) {5} translate('{6}' using UTF8TOUCS2) " +
            "and (ob.OBJECT_TYPE in ('BT', 'VI')) " +
            "and (trim(co.COLUMN_CLASS) not in ('S', 'M')) " +
            "FOR READ UNCOMMITTED ACCESS ORDER BY 1, 2, 3, co.COLUMN_NUMBER;";
     

        private static string PrimaryKeyQuery = "select " +
            "cast ( '{0}' as varchar(128) ) CatalogName," +
            "cast(trim(ob.SCHEMA_NAME) as varchar(128) ) SchemaName," +
            "cast(trim(ob.OBJECT_NAME) as varchar(128) ) TableName," +
            "trim(ky.COLUMN_NAME) ColumnName " +
            " from TRAFODION.\"_MD_\".OBJECTS ob, " +
            "TRAFODION.\"_MD_\".KEYS ky " +
            " where ob.SCHEMA_NAME = '{2}'" +
            " and ob.OBJECT_NAME = '{4}' " +
            " and ob.OBJECT_UID = ky.OBJECT_UID and ky.COLUMN_NAME <> '_SALT_' " +
            " FOR READ UNCOMMITTED ACCESS";
        
        private static string ForeignKeyQuery = "SELECT T1.CAT_UID, T2.SCHEMA_UID, T3.UNIQUE_CONSTRAINT_UID " +
                    "FROM {0}.\"_MD_\".CATSYS AS T1, " +
                        "{0}.\"_MD_\".SCHEMATA AS T2, " +
                        "{1}.HP_DEFINITION_SCHEMA.REF_CONSTRAINTS AS T3 " +
                    "WHERE T3.CONSTRAINT_UID = {2} " +
                        "AND T2.SCHEMA_UID = T3.UNIQUE_CONSTRAINT_SCH_UID " +
                        "AND T1.CAT_UID = T3.UNIQUE_CONSTRAINT_CAT_UID " +
                        "FOR READ UNCOMMITTED ACCESS;";
        private static string ForeignKeyColumns = "SELECT T2.OBJECT_NAME, T2.OBJECT_UID " +
                    "FROM {0}.HP_DEFINITION_SCHEMA.TBL_CONSTRAINTS AS T1, " +
                    "{0}.HP_DEFINITION_SCHEMA.OBJECTS AS T2 " +
                    "WHERE T1.CONSTRAINT_UID =  {1} " +
                    "AND T1.TABLE_UID = T2.OBJECT_UID " +
                    "FOR READ UNCOMMITTED ACCESS;";

        private static DataSet _dataset;

        private TrafDbConnection _conn;
        private string _segment;

        public TrafDbMetaData(TrafDbConnection conn)
        {
            _conn = conn;
            _dataset = null;

            //since every connection will create a metadata object
            //no need to initialize everything for now, wait until someone asks for something
        }

        private void Init()
        {
            if (_conn.ByteOrder == ByteOrder.LittleEndian)
            {
                _segment = "NSK";
            }
            else
            {
                _segment = _conn.RemoteProcess.Substring(0, 7);
            }

            if (_dataset == null)
            {
                _dataset = new DataSet("MetadataCollections");

                //documents say that using .BeginLoadData() and .EndLoadData() can speed up read access
                //but you cant use this while reading in the schema -- i think our data is small enough not to matter
                //maybe read schema in first from one file, BeginLoadData, then read data in if performance becomes poor
                _dataset.ReadXml(Assembly.GetExecutingAssembly().GetManifestResourceStream("Trafodion.Data.Resources.Metadata.xml"), XmlReadMode.ReadSchema);
            }
        }

        /// <summary>
        /// This is the generic way users will ask for metadata information
        /// </summary>
        /// <param name="collectionName"></param>
        /// <param name="restrictionValues"></param>
        /// <returns></returns>
        public DataTable GetSchema(string collectionName, string[] restrictionValues)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                string[] p = new string[restrictionValues.Length + 1];
                p[0] = collectionName;
                Array.Copy(restrictionValues, 0, p, 1, restrictionValues.Length);

                TrafDbTrace.Trace(this._conn, TraceLevel.Internal, p);
            }

            DataTable dt = null;

            collectionName = collectionName.ToUpper();

            if (this._segment == null)
            {
                Init();
            }

            switch (collectionName)
            {
                //static queries
                case "METADATACOLLECTIONS":
                    dt = _dataset.Tables["MetadataCollections"].Copy();
                    break;
                case "DATASOURCEINFORMATION":
                    dt = _dataset.Tables["DataSourceInformation"].Copy();
                    //few dynamic elements
                    dt.Rows[0]["DataSourceProductVersion"] = "2.5";
                    dt.Rows[0]["DataSourceProductVersionNormalized"] = "02.05.0000";
                    break;
                case "DATATYPES":
                    dt = _dataset.Tables["DataTypes"].Copy();
                    break;
                case "RESTRICTIONS":
                    dt = _dataset.Tables["Restrictions"].Copy();
                    break;
                case "RESERVEDWORDS":
                    dt = _dataset.Tables["ReservedWords"].Copy();
                    break;

                //dynamic
                case "CATALOGS":
                    dt = GetCatalogs(restrictionValues);
                    break;
                case "SCHEMAS":
                    dt = GetSchemas(restrictionValues);
                    break;
                case "TABLES":
                    dt = GetObjects(restrictionValues, TableQuery);
                    break;
                case "VIEWS":
                    dt = GetObjects(restrictionValues, ViewQuery);
                    break;
                case "INDEXES":
                    dt = GetObjects(restrictionValues, IndexQuery);
                    break;
                case "COLUMNS":
                    dt = GetColumns(restrictionValues);
                    break;
                case "PRIMARYKEYS":
                    dt = GetPrimaryKeys(restrictionValues);
                    break;
                case "PROCEDURES":
                case "FOREIGNKEYS":
                case "FOREIGNKEYCOLUMNS":
                default:
                    TrafDbException.ThrowException(this._conn, new InvalidOperationException("Invalid Collection Name: " + collectionName));
                    break;
            }

            return dt;
        }

        private DataTable ExecuteQuery(string query)
        {
            if (TrafDbTrace.IsInternalEnabled)
            {
                TrafDbTrace.Trace(this._conn, TraceLevel.Internal, query);
            }

            DataTable dt;

            using (TrafDbCommand cmd = this._conn.CreateCommand())
            {
                cmd.CommandText = query;
                using (TrafDbDataAdapter da = new TrafDbDataAdapter(cmd))
                {
                    dt = new DataTable();
                    da.Fill(dt);
                }
            }

            return dt;
        }

        private void CopyGenericRestrictions(string[] source, int sourceOffset, object[] dest, int destOffset, int count)
        {
            for (int i = 0; i < count; i++, sourceOffset++)
            {
                if (source.Length <= sourceOffset || source[sourceOffset] == null)
                {
                    dest[destOffset++] = "LIKE";
                    dest[destOffset++] = "%";
                }
                else
                {
                    dest[destOffset++] = TrafDbUtility.FindWildCards.IsMatch(source[sourceOffset]) ? "LIKE" : "=";
                    dest[destOffset++] = source[sourceOffset];
                }
            }
        }

        private DataTable GetCatalogs(string[] restrictions)
        {
            object[] res = new object[2];

            CopyGenericRestrictions(restrictions, 0, res, 0, 1);

            return ExecuteQuery(String.Format(CatalogQuery, res));
        }

        private DataTable GetSchemas(string[] restrictions)
        {
            object[] res = new object[4];

            CopyGenericRestrictions(restrictions, 0, res, 0, 2);
   
            return ExecuteQuery(String.Format(SchemaQuery, res));
        }

        private DataTable GetObjects(string[] restrictions, string query)
        {
            object[] res = new object[5];
            res[0] = (restrictions.Length < 1 || String.IsNullOrEmpty(restrictions[0]))?_conn.Database:restrictions[0];

            CopyGenericRestrictions(restrictions, 1, res, 1, 2);

            return ExecuteQuery(String.Format(query, res));
        }

        private DataTable GetColumns(string[] restrictions)
        {
            object[] res = new object[7];
            res[0] = (restrictions.Length < 1 || String.IsNullOrEmpty(restrictions[0])) ? _conn.Database : restrictions[0];

            CopyGenericRestrictions(restrictions, 1, res, 1, 3);

            return ExecuteQuery(String.Format(ColumnQuery, res));
        }

        private DataTable GetPrimaryKeys(string[] restrictions)
        {
            object[] res = new object[5];
            res[0] = (restrictions.Length < 1 || String.IsNullOrEmpty(restrictions[0])) ? _conn.Database : restrictions[0];

            CopyGenericRestrictions(restrictions, 1, res, 1, 2);

            return ExecuteQuery(String.Format(PrimaryKeyQuery, res));
        }
    }
}



// saving this code here for when we update reserved words
/*public static void CreateReservedWordList()
{
    TextReader tr = new StreamReader("c:\\keywords.txt");
    TextWriter wr = new StreamWriter("c:\\output.txt");

    string str = tr.ReadToEnd();
    tr.Close();

    string[] words = str.Split(new char[] { '\r', '\n', ' ' });
    Array.Sort(words);
    foreach (string s in words)
    {
        if (s.Length > 0)
        {
            wr.WriteLine(string.Concat("<ReservedWords><ReservedWords>", s, "</ReservedWords></ReservedWords>"));
        }
    }

    wr.Flush();
    wr.Close();

}*/