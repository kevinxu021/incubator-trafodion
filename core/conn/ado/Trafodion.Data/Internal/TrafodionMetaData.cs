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

       private static string ObjectQuery = @"
select 
    rtrim(cat.cat_name) as CatalogName, 
    rtrim(sch.schema_name) as SchemaName, 
    rtrim(obj.object_name) as TableName 
from 
    TRAFODION.""_MD_"".CATSYS as cat inner join
    TRAFODION.""_MD_"".SCHEMATA as sch on cat.cat_uid = sch.cat_uid inner join 
    {0}.HP_DEFINITION_SCHEMA.OBJECTS as obj on sch.schema_uid = obj.schema_uid
where 
    rtrim(cat.cat_name) = translate('{0}' using UTF8TOUCS2) and 
    rtrim(sch.schema_name) {1} translate('{2}' using UTF8TOUCS2) and 
    rtrim(obj.object_name) {3} translate('{4}' using UTF8TOUCS2) and 
    obj.object_security_class = 'UT'
";
        private static string TableQuery = ObjectQuery + "and obj.object_type = 'BT' and obj.object_name_space = 'TA' for read uncommitted access";
        private static string ViewQuery = ObjectQuery + "and (obj.object_type = 'VI' or obj.object_type = 'MV') for read uncommitted access";
        private static string IndexQuery = ObjectQuery + "and obj.object_type = 'IX' and obj.object_name_space = 'IX' for read uncommitted access";

        private static string ProcedureQuery = "";

        private static string ColumnQuery = @"
select 
    rtrim(cat.cat_name) as CatalogName, 
    rtrim(sch.schema_name) as SchemaName, 
    rtrim(obj.object_name) as TableName, 
    rtrim(cols.column_name) as ColumnName, 
    cols.sql_data_type as DataType, 
    cols.column_size as ColumnSize, 
    cols.col_precision as ColumnPrecision, 
    cols.col_scale as ColumnScale, 
    cols.column_number as Ordinal, 
    cols.default_value as DefaultValue,
    cols.character_set as CharacterSet,
    cols.encoding as Encoding
from 
    TRAFODION.""_MD_"".CATSYS as cat inner join
    TRAFODION.""_MD_"".SCHEMATA as sch on cat.cat_uid = sch.cat_uid inner join
    {0}.HP_DEFINITION_SCHEMA.OBJECTS as obj on sch.schema_uid = obj.schema_uid inner join
    {0}.HP_DEFINITION_SCHEMA.COLS as cols on obj.object_uid = cols.object_uid
where 
    rtrim(cat.cat_name) = translate('{0}' using UTF8TOUCS2) and 
    rtrim(sch.schema_name) {1} translate('{2}' using UTF8TOUCS2) and 
    rtrim(obj.object_name) {3} translate('{4}' using UTF8TOUCS2) and 
    rtrim(cols.column_name) {5} translate('{6}' using UTF8TOUCS2) and 
    cols.column_class = 'U'
order by 
    cols.column_number
for read uncommitted access";

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
                    dest[destOffset++] = HPDbUtility.FindWildCards.IsMatch(source[sourceOffset]) ? "LIKE" : "=";
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