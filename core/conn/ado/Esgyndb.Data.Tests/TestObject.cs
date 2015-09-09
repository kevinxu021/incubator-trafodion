using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;
using System.Configuration;
using NUnit.Framework;
using Esgyndb.Data;

namespace TrafAdoTest
{
    [SetUpFixture]
    public class AdoTestSetup
    {
        public static TestObject testInfo;

        [SetUp]
        public void SetUp()
        {
            testInfo = new TestObject("ASCII");
            if (ConfigurationManager.AppSettings["CreateDB"].Equals("true"))
                testInfo.SetupTest();
        }

        [TearDown]
        public void TearDown()
        {
            testInfo = null;
        }
    }

    public class TrafErrorHelper
    {
        public void HandleInfoEvents(object sender, EsgyndbInfoMessageEventArgs e)
        {
            foreach (EsgyndbError oEvent in e.Errors)
            {
                //    continue;
                Console.WriteLine("============================================================");
                Console.WriteLine("Even kicked in:");
                Console.WriteLine("Errors: " + oEvent.Message);
                Console.WriteLine("ErrorCode: " + oEvent.ErrorCode);
                Console.WriteLine("RowId: " + oEvent.RowId);
                Console.WriteLine("State: " + oEvent.State);
                Console.WriteLine("============================================================");
            }
        }

        //public void da_FillError(object sender, FillErrorEventArgs e)
        //{
        //    Exception ex = e.Errors;
        //    Console.WriteLine("============================================================");
        //    Console.WriteLine("Fill Error:");
        //    Console.WriteLine("Errors: " + ex.Data);
        //    Console.WriteLine("SQLState: " + ex.Message);
        //    Console.WriteLine("Native: " + ex.StackTrace);
        //    Console.WriteLine("Source: " + ex.Source);
        //    Console.WriteLine("============================================================");
        //}
    }

    public struct aColumn
    {
        public string columnName;
        public int size;
        public int precision;
        public int scale;
        public string inValue;
        public string expValue;
        public EsgyndbType dbType;
        public string systemType;
        public string expectedError;
        public string actutalError;
        public bool errorFound;

        public aColumn(string name, int size, int prec, int scale, string inVal, string expVal, EsgyndbType dbType, string systemType)
        {
            this.columnName = name;
            this.size = size;
            this.precision = prec;
            this.scale = scale;
            this.inValue = inVal;
            this.expValue = expVal;
            this.dbType = dbType;
            this.systemType = systemType;
            this.expectedError = "";
            this.actutalError = "";
            this.errorFound = false;
        }
    }

    public class TestObject
    {
        private List<string> debugError = new List<string>();

        private CharsetData charset_bank;
        private List<List<aColumn>> tableInfo;

        public string connectionString;
        public EsgyndbConnection connection;
        public string charset = "ASCII";

        public string tableName;
        public string dropTable;
        public string createTable;
        public string insertTable;
        public string selectTable;
        public string updateTable;
        public string deleteTable;

        public string[] data_bank_iso = new string[1000];
        public string[] data_bank_charset = new string[1000];
        public string[] name_bank = new string[60];

        public TestObject(string charset)
        {
            this.connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            Console.WriteLine("connectionString = " + connectionString);

            this.charset = charset;

            this.connection = new EsgyndbConnection(connectionString);
            TrafErrorHelper eeh = new TrafErrorHelper();
            this.connection.InfoMessage += new EsgyndbInfoMessageEventHandler(eeh.HandleInfoEvents);

            this.tableInfo = new List<List<aColumn>>();
            this.charset_bank = new CharsetData(this.charset);

            //byte [] str = this.charset_bank.getNativeChars();
            //int i = str.Length;// GBK: 21888 characters; SJIS:7883

            generate_data_bank();
            generate_data();
            init_DML_DDL("ADONET_TABLE");
        }

        private void generate_data_bank()
        {
            char achar;
            data_bank_iso[0] = Convert.ToChar(33).ToString();
            for (int i = 1; i < this.data_bank_iso.Length; i++)
            {
                achar = Convert.ToChar((i % 95) + 33);
                if (achar == '\'')
                    data_bank_iso[i] = data_bank_iso[i - 1] + "a";
                else
                    data_bank_iso[i] = data_bank_iso[i - 1] + achar.ToString();
            }

            switch (charset)
            {
                case "ASCII":
                    this.data_bank_charset = (string[])this.data_bank_iso.Clone();
                    break;
                case "SJIS":
                    for (int i = 0; i < this.data_bank_charset.Length; i++)
                    {
                        data_bank_charset[i] = this.charset_bank.getUnicodeString(i - (i % 100), i + 1);
                    }
                    break;
                case "GBK":
                case "GB2":
                    for (int i = 0; i < this.data_bank_charset.Length; i++)
                    {
                        data_bank_charset[i] = this.charset_bank.getUnicodeString(i - (i % 25), i + 1);
                    }
                    break;
                default:
                    Console.WriteLine("INTERNAL ERROR: No charset option found for {0}.", charset);
                    break;
            }

            //System.IO.StreamWriter sw1 = new System.IO.StreamWriter("gbk.txt", false, Encoding.Unicode);
            //sw1.WriteLine(this.charset_bank.getUnicodeString());
            //sw1.Close();

            //Create all the column names
            if (charset.Equals("ASCII"))
            {
                for (int i = 0; i < this.name_bank.Length; i++)
                {
                    if (i < 9) this.name_bank[i] = "C0" + Convert.ToString(i + 1);
                    else this.name_bank[i] = "C" + Convert.ToString(i + 1);
                }
            }
            else
            {
                UTF8Encoding myutf8 = new UTF8Encoding();
                int index = 0;
                int count = 0;
                for (int i = 0; i < this.name_bank.Length; i++)
                {
                    count = 0;
                    string str = "";
                    while (count < 126)
                    {
                        string myStr = this.charset_bank.getUnicodeString(index, 1);
                        str += myStr;
                        index++;
                        count += myutf8.GetByteCount(myStr);

                        if (i < 9) this.name_bank[i] = "\"" + str + "0" + Convert.ToString(i + 1) + "\"";
                        else this.name_bank[i] = "\"" + str + Convert.ToString(i + 1) + "\"";
                    }
                }
            }
        }

        private void generate_data()
        {
            this.tableInfo.Clear();
            for (int i = 1; i <= 10; i++)
            {
                List<aColumn> aRow = new List<aColumn>();

                aRow.Add(new aColumn(name_bank[0], 500, 0, 0,
                    data_bank_iso[i * 50 - 1],
                    data_bank_iso[i * 50 - 1] + spacePadding(500 - data_bank_iso[i * 50 - 1].Length),
                    EsgyndbType.Char, "String"));//C01 500

                aRow.Add(new aColumn(name_bank[1], 500, 0, 0,
                    data_bank_charset[i * 50 - 1],
                    data_bank_charset[i * 50 - 1] + spacePadding(500 - data_bank_charset[i * 50 - 1].Length),
                    EsgyndbType.Char, "String"));//C02 500

                aRow.Add(new aColumn(name_bank[2], 100, 0, 0,
                    data_bank_iso[i * 10 - 1],
                    data_bank_iso[i * 10 - 1],
                    EsgyndbType.Varchar, "String"));//C03 100

                aRow.Add(new aColumn(name_bank[3], 100, 0, 0,
                    data_bank_charset[i * 10 - 1],
                    data_bank_charset[i * 10 - 1],
                    EsgyndbType.Varchar, "String"));//C04 100

                aRow.Add(new aColumn(name_bank[4], 2000, 0, 0,
                    data_bank_iso[i * 100 - 1],
                    data_bank_iso[i * 100 - 1],
                    EsgyndbType.Varchar, "String"));//C05 2000

                aRow.Add(new aColumn(name_bank[5], 2000, 0, 0,
                    data_bank_charset[i * 100 - 1],
                    data_bank_charset[i * 100 - 1],
                    EsgyndbType.Varchar, "String"));//C06 2000

                aRow.Add(new aColumn(name_bank[6], 1000, 0, 0,
                   data_bank_charset[i * 100 - 1],
                   data_bank_charset[i * 100 - 1] + spacePadding(1000 - data_bank_charset[i * 100 - 1].Length),
                   EsgyndbType.Char, "String"));//C07 1000

                aRow.Add(new aColumn(name_bank[7], 1000, 0, 0,
                  data_bank_charset[i * 100 - 1],
                  data_bank_charset[i * 100 - 1],
                  EsgyndbType.Varchar, "String"));//C08 1000

                aRow.Add(new aColumn(name_bank[8], 0, 9, 6, "-123.123456", "-123.123456", EsgyndbType.Decimal, "Decimal"));//C09 decimal signed
                aRow.Add(new aColumn(name_bank[9], 0, 9, 6, "123.123456", "123.123456", EsgyndbType.DecimalUnsigned, "Decimal"));//C10 decimal unsigned
                aRow.Add(new aColumn(name_bank[10], 0, 18, 6, "-1234567.123456", "-1234567.123456", EsgyndbType.Numeric, "Decimal"));//C11 numeric signed
                aRow.Add(new aColumn(name_bank[11], 0, 9, 6, "123.123456", "123.123456", EsgyndbType.NumericUnsigned, "Decimal"));//C12 numeric unsigned
                //aRow.Add(new aColumn(name_bank[12], 0, 0, 0, "-123", "-123", EsgyndbType.SmallInt, "Int16"));//13 tinyint signed  -> Esgyndb .NET Provider doesn't support tinyint signed
                aRow.Add(new aColumn(name_bank[12], 0, 0, 0, "123", "123", EsgyndbType.SmallInt, "Int16"));//13 tinyint signed  -> Esgyndb .NET Provider doesn't support tinyint signed
                aRow.Add(new aColumn(name_bank[13], 0, 0, 0, "123", "123", EsgyndbType.SmallIntUnsigned, "UInt16"));//C14 tinyint unsigned
                aRow.Add(new aColumn(name_bank[14], 0, 0, 0, "-123", "-123", EsgyndbType.SmallInt, "Int16"));//C15 smallint signed
                aRow.Add(new aColumn(name_bank[15], 0, 0, 0, "123", "123", EsgyndbType.SmallIntUnsigned, "UInt16"));//C16 smallint unsigned
                aRow.Add(new aColumn(name_bank[16], 0, 0, 0, "-12345678", "-12345678", EsgyndbType.Integer, "Int32"));//C17 integer signed
                aRow.Add(new aColumn(name_bank[17], 0, 0, 0, "12345678", "12345678", EsgyndbType.IntegerUnsigned, "UInt32"));//C18 integer unsigned
                aRow.Add(new aColumn(name_bank[18], 0, 0, 0, i.ToString(), i.ToString(), EsgyndbType.LargeInt, "Int64"));//C19 largeint
                aRow.Add(new aColumn(name_bank[19], 0, 0, 0, "-0.567", "-0.567", EsgyndbType.Real, "Single"));//C20 real
                aRow.Add(new aColumn(name_bank[20], 0, 54, 0, "-0.1234567", "-0.1234567", EsgyndbType.Float, "Double"));//C21 float
                aRow.Add(new aColumn(name_bank[21], 0, 0, 0, "0.12345678901234499", "0.123456789012345", EsgyndbType.Double, "Double"));//C22 double precision
                aRow.Add(new aColumn(name_bank[22], 0, 0, 0, "2008-04-03", "2008-04-03", EsgyndbType.Date, "DateTime"));//C23 date
                aRow.Add(new aColumn(name_bank[23], 0, 0, 0, "19:12:10", "19:12:10", EsgyndbType.Time, "TimeSpan"));//C24 time
                aRow.Add(new aColumn(name_bank[24], 0, 0, 0, "2008-04-03 19:12:10.123", "2008-04-03 19:12:10.123000", EsgyndbType.Timestamp, "DateTime"));//C25 timestamp
                aRow.Add(new aColumn(name_bank[25], 0, 128, 32, "-1.1234567890123456789012345678901234567890", "-1.12345678901234567890123456789012", EsgyndbType.Numeric, "Decimal"));//C26 bignum signed
                aRow.Add(new aColumn(name_bank[26], 0, 128, 32, "1.1234567890123456789012345678901234567890", "1.12345678901234567890123456789012", EsgyndbType.NumericUnsigned, "Decimal"));//C27 bignum unsigned
                aRow.Add(new aColumn(name_bank[27], 0, 0, 0, "2", "2", EsgyndbType.Interval, "String"));//C28 INTERVAL YEAR
                aRow.Add(new aColumn(name_bank[28], 0, 0, 0, "2", "2", EsgyndbType.Interval, "String"));//C29 INTERVAL MONTH
                aRow.Add(new aColumn(name_bank[29], 0, 0, 0, "2", "2", EsgyndbType.Interval, "String"));//C30 INTERVAL DAY
                aRow.Add(new aColumn(name_bank[30], 0, 0, 0, "2", "2", EsgyndbType.Interval, "String"));//C31 INTERVAL HOUR
                aRow.Add(new aColumn(name_bank[31], 0, 0, 0, "2", "2", EsgyndbType.Interval, "String"));//C32 INTERVAL MINUTE
                aRow.Add(new aColumn(name_bank[32], 0, 0, 0, "2", "2.000000", EsgyndbType.Interval, "String"));//C33 INTERVAL SECOND
                aRow.Add(new aColumn(name_bank[33], 0, 0, 0, "02-07", "2-07", EsgyndbType.Interval, "String"));//C34 INTERVAL YEAR TO MONTH
                aRow.Add(new aColumn(name_bank[34], 0, 0, 0, "63 12", "63 12", EsgyndbType.Interval, "String"));//C35 INTERVAL DAY TO HOUR
                aRow.Add(new aColumn(name_bank[35], 0, 0, 0, "63 12:39", "63 12:39", EsgyndbType.Interval, "String"));//C36 DAY TO MINUTE
                aRow.Add(new aColumn(name_bank[36], 0, 0, 0, "63 12:39:59.163000", "63 12:39:59.163000", EsgyndbType.Interval, "String"));//C37 DAY TO SECOND
                aRow.Add(new aColumn(name_bank[37], 0, 0, 0, "63:39", "63:39", EsgyndbType.Interval, "String"));//C38 INTERVAL HOUR TO MINUTE
                aRow.Add(new aColumn(name_bank[38], 0, 0, 0, "63:39:59.163000", "63:39:59.163000", EsgyndbType.Interval, "String"));//C39 INTERVAL HOUR TO SECOND
                aRow.Add(new aColumn(name_bank[39], 0, 0, 0, "39:59.163000", "39:59.163000", EsgyndbType.Interval, "String"));//C40 INTERVAL MINUTE TO SECOND
                aRow.Add(new aColumn(name_bank[40], 20, 0, 0, "null", string.Empty, EsgyndbType.Char, "String"));//C41 CHAR(20)
                aRow.Add(new aColumn(name_bank[41], 20, 0, 0, "null", string.Empty, EsgyndbType.Char, "String"));//C42 NCHAR(20)
                aRow.Add(new aColumn(name_bank[42], 20, 0, 0, "null", string.Empty, EsgyndbType.Varchar, "String"));//C43 VARCHAR(20)
                aRow.Add(new aColumn(name_bank[43], 20, 0, 0, "null",string.Empty, EsgyndbType.Char, "String"));//C44 NVARCHAR(20)
                aRow.Add(new aColumn(name_bank[44], 0, 0, 0, "null", string.Empty, EsgyndbType.Integer, "Int32"));//C45 INTEGER
                this.tableInfo.Add(aRow);
            }
        }

        private void init_DML_DDL(string tableName)
        {
            this.tableName = tableName;

            this.createTable =
                "CREATE TABLE " + this.tableName + " ( " +
                name_bank[0] + " CHAR( 500 ) CHARACTER SET ISO88591 NOT NULL, " +
                name_bank[1] + " CHAR( 500 ) CHARACTER SET UCS2 NOT NULL, " +
                name_bank[2] + " VARCHAR( 100 ) CHARACTER SET ISO88591, " +
                name_bank[3] + " VARCHAR( 100 ) CHARACTER SET UCS2 NOT NULL, " +
                name_bank[4] + " LONG VARCHAR CHARACTER SET ISO88591, " +
                name_bank[5] + " LONG VARCHAR CHARACTER SET UCS2, " +
                name_bank[6] + " NCHAR( 1000 ), " +
                name_bank[7] + " NCHAR VARYING( 1000 ), " +
                name_bank[8] + " DECIMAL (9, 6) SIGNED, " +
                name_bank[9] + " DECIMAL (9, 6) UNSIGNED, " +
                name_bank[10] + " NUMERIC (18, 6) SIGNED, " +
                name_bank[11] + " NUMERIC (9, 6) UNSIGNED, " +
                name_bank[12] + " TINYINT SIGNED, " +
                name_bank[13] + " TINYINT UNSIGNED, " +
                name_bank[14] + " SMALLINT SIGNED, " +
                name_bank[15] + " SMALLINT UNSIGNED, " +
                name_bank[16] + " INTEGER SIGNED NOT NULL, " +
                name_bank[17] + " INTEGER UNSIGNED, " +
                name_bank[18] + " LARGEINT NOT NULL NOT DROPPABLE, " +
                name_bank[19] + " REAL, " +
                name_bank[20] + " FLOAT(54), " +
                name_bank[21] + " DOUBLE PRECISION, " +
                name_bank[22] + " DATE, " +
                name_bank[23] + " TIME, " +
                name_bank[24] + " TIMESTAMP, " +
                name_bank[25] + " NUMERIC (128, 32) SIGNED, " +
                name_bank[26] + " NUMERIC (128, 32) UNSIGNED, " +
                name_bank[27] + " INTERVAL YEAR, " +
                name_bank[28] + " INTERVAL MONTH, " +
                name_bank[29] + " INTERVAL DAY, " +
                name_bank[30] + " INTERVAL HOUR, " +
                name_bank[31] + " INTERVAL MINUTE, " +
                name_bank[32] + " INTERVAL SECOND, " +
                name_bank[33] + " INTERVAL YEAR TO MONTH, " +
                name_bank[34] + " INTERVAL DAY TO HOUR, " +
                name_bank[35] + " INTERVAL DAY TO MINUTE, " +
                name_bank[36] + " INTERVAL DAY TO SECOND, " +
                name_bank[37] + " INTERVAL HOUR TO MINUTE, " +
                name_bank[38] + " INTERVAL HOUR TO SECOND, " +
                name_bank[39] + " INTERVAL MINUTE TO SECOND, " +
                name_bank[40] + " CHAR( 20 ) CHARACTER SET ISO88591, " + //Testing DbNull
                name_bank[41] + " CHAR( 20 ) CHARACTER SET UCS2, " + //Testing DbNull
                name_bank[42] + " VARCHAR( 20 ) CHARACTER SET ISO88591, " + //Testing DbNull
                name_bank[43] + " VARCHAR( 20 ) CHARACTER SET UCS2, " + //Testing DbNull
                name_bank[44] + " INTEGER, " + //Testing DbNull
                "PRIMARY KEY (" + name_bank[18] + "), CONSTRAINT CHECK_" + this.tableName + " CHECK ( " +
                name_bank[16] + " < 50000 ) )";

            this.updateTable = 
                "UPDATE " + this.tableName + " SET " +
                name_bank[0] + "=?, " +
                name_bank[1] + "=?, " +
                name_bank[2] + "=?, " +
                name_bank[3] + "=?, " +
                name_bank[4] + "=?, " +
                name_bank[5] + "=?, " +
                name_bank[6] + "=?, " +
                name_bank[7] + "=?, " +
                name_bank[8] + "=?, " +
                name_bank[9] + "=?, " +
                name_bank[10] + "=?, " +
                name_bank[11] + "=?, " +
                name_bank[12] + "=?, " +
                name_bank[13] + "=?, " +
                name_bank[14] + "=?, " +
                name_bank[15] + "=?, " +
                name_bank[16] + "=? + 1, " +
                name_bank[17] + "=?, " +
                name_bank[19] + "=?, " +
                name_bank[20] + "=?, " +
                name_bank[21] + "=?, " +
                name_bank[22] + "=?, " +
                name_bank[23] + "=?, " +
                name_bank[24] + "=?, " +
                name_bank[25] + "=?, " +
                name_bank[26] + "=?, " +
                name_bank[27] + "=?, " +
                name_bank[28] + "=?, " +
                name_bank[29] + "=?, " +
                name_bank[30] + "=?, " +
                name_bank[31] + "=?, " +
                name_bank[32] + "=?, " +
                name_bank[33] + "=?, " +
                name_bank[34] + "=?, " +
                name_bank[35] + "=?, " +
                name_bank[36] + "=?, " +
                name_bank[37] + "=?, " +
                name_bank[38] + "=?, " +
                name_bank[39] + "=?, " +
                name_bank[40] + "=?, " +
                name_bank[41] + "=?, " +
                name_bank[42] + "=?, " +
                name_bank[43] + "=?, " +
                name_bank[44] + "=?, " +
                "WHERE " + name_bank[18] + " = ?";

            this.insertTable = "INSERT INTO " + this.tableName + " VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
            this.selectTable = "SELECT * FROM " + this.tableName + " ORDER BY " + name_bank[18];
            this.deleteTable = "DELETE FROM " + this.tableName;
            this.dropTable = "DROP TABLE " + this.tableName + " CASCADE";
        }

        private void insert_rows(EsgyndbCommand cmd, string tName)
        {
            try
            {
                foreach (List<aColumn> aRow in this.tableInfo)
                {
                    string str = "INSERT INTO " + tName + " VALUES (";
                    int i = 1;
                    foreach (aColumn aCol in aRow)
                    {
                        switch (i)
                        {
                            case 1:
                            case 3:
                            case 5:
                                str += "_ISO88591'" + aCol.inValue + "'";
                                break;
                            case 2:
                            case 4:
                            case 6:
                            case 7:
                            case 8:
                                str += "_UCS2'" + aCol.inValue + "'";
                                break;
                            case 23:
                                str += "{d '" + aCol.inValue + "'}";
                                break;
                            case 24:
                                str += "{t '" + aCol.inValue + "'}";
                                break;
                            case 25:
                                str += "{ts '" + aCol.inValue + "'}";
                                break;
                            case 28:
                                str += "INTERVAL '" + aCol.inValue + "' YEAR ";
                                break;
                            case 29:
                                str += "INTERVAL '" + aCol.inValue + "' MONTH ";
                                break;
                            case 30:
                                str += "INTERVAL '" + aCol.inValue + "' DAY ";
                                break;
                            case 31:
                                str += "INTERVAL '" + aCol.inValue + "' HOUR ";
                                break;
                            case 32:
                                str += "INTERVAL '" + aCol.inValue + "' MINUTE ";
                                break;
                            case 33:
                                str += "INTERVAL '" + aCol.inValue + "' SECOND ";
                                break;
                            case 34:
                                str += "INTERVAL '" + aCol.inValue + "' YEAR TO MONTH ";
                                break;
                            case 35:
                                str += "INTERVAL '" + aCol.inValue + "' DAY TO HOUR ";
                                break;
                            case 36:
                                str += "INTERVAL '" + aCol.inValue + "' DAY TO MINUTE ";
                                break;
                            case 37:
                                str += "INTERVAL '" + aCol.inValue + "' DAY TO SECOND ";
                                break;
                            case 38:
                                str += "INTERVAL '" + aCol.inValue + "' HOUR TO MINUTE ";
                                break;
                            case 39:
                                str += "INTERVAL '" + aCol.inValue + "' HOUR TO SECOND ";
                                break;
                            case 40:
                                str += "INTERVAL '" + aCol.inValue + "' MINUTE TO SECOND ";
                                break;
                            default:
                                str += aCol.inValue;
                                break;
                        }

                        if (i != aRow.Count) str += ", ";
                        i++;
                    }
                    str += ");";

                    Console.WriteLine(str);
                    cmd.CommandText = str;
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public List<List<aColumn>> getTableInfo()
        {
            return this.tableInfo;
        }

        public void SetupTest()
        {
            try
            {
                using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
                {
                    //conn.ConnectionTimeout = 120;
                    TrafErrorHelper eeh = new TrafErrorHelper();
                    conn.InfoMessage += new EsgyndbInfoMessageEventHandler(eeh.HandleInfoEvents);

                    conn.Open();
                    EsgyndbCommand cmd = conn.CreateCommand();
                    cmd.CommandTimeout = 0;

                    //Cleanup
                    try
                    {
                        cmd.CommandText = this.dropTable;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception)
                    {
                    }

                    //Create table
                    cmd.CommandText = this.createTable;
                    //Console.WriteLine(cmd.CommandText);
                    int rowAffected = cmd.ExecuteNonQuery();

                    insert_rows(cmd, this.tableName);

                    conn.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public void ReinitializeTest()
        {
            
            //generate_data();

            try
            {
                using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
                {
                    TrafErrorHelper eeh = new TrafErrorHelper();
                    conn.InfoMessage += new EsgyndbInfoMessageEventHandler(eeh.HandleInfoEvents);

                    conn.Open();
                    EsgyndbCommand cmd = conn.CreateCommand();
                    cmd.CommandTimeout = 0;

                    //Create table
                    cmd.CommandText = this.deleteTable;
                    int rowAffected = cmd.ExecuteNonQuery();

                    insert_rows(cmd, this.tableName);

                    conn.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public void CleanupTest()
        {
            try
            {
                using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
                {
                    TrafErrorHelper eeh = new TrafErrorHelper();
                    conn.InfoMessage += new EsgyndbInfoMessageEventHandler(eeh.HandleInfoEvents);

                    conn.Open();
                    EsgyndbCommand cmd = conn.CreateCommand();

                    //Drop table
                    cmd.CommandText = this.dropTable;
                    cmd.ExecuteNonQuery();

                    conn.Close();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        //public bool CompareData(DataTable table)
        //{
        //    bool pass = true;

        //    if (table.Rows.Count != this.tableInfo.Count)
        //    {
        //        pass = false;
        //        Helper.DisplayMessage(Helper.msgType.ERR, "Number of rows mismatched.");
        //        return pass;
        //    }
        //    if (table.Columns.Count != this.tableInfo[0].Count)
        //    {
        //        pass = false;
        //        Helper.DisplayMessage(Helper.msgType.ERR, "Number of columns mismatched.");
        //        return pass;
        //    }

        //    int j = 0;
        //    foreach (ArrayList aRow in this.tableInfo)
        //    {
        //        for (int i = 0; i < table.Columns.Count; i++)
        //        {
        //            string expVal = ((aColumn)aRow[i]).expValue;
        //            string actVal = table.Rows[j][i].ToString();

        //            switch (i + 1)
        //            {
        //                case 23:
        //                case 25:
        //                    expVal = Convert.ToDateTime(expVal).ToString();
        //                    break;
        //                default:
        //                    break;
        //            }

        //            if (!expVal.Equals(actVal))
        //            {
        //                pass = false;
        //                Helper.DisplayMessage(Helper.msgType.ERR, "Row " + (j + 1) + ", Column " + (i + 1) + ": expected: ==" + expVal + "==, actual: ==" + actVal + "==");
        //                Helper.DisplayMessage(Helper.msgType.NONE, "=================================================================================");
        //            }
        //            else
        //            {
        //                //Helper.DisplayMessage(Helper.msgType.NONE, "expected: ==" + expVal + "==, actual: ==" + actVal + "==");
        //            }
        //        }

        //        j++;
        //    }

        //    return pass;
        //}

        //public bool CompareData(EsgyndbDataReader reader, CommandBehavior cmdBehaviour, Helper.fetchType fType)
        //{
        //    bool pass = true;
        //    ArrayList firstRow = (ArrayList)this.tableInfo[0];

        //    switch (cmdBehaviour)
        //    {
        //        case CommandBehavior.SingleRow:
        //            if (!reader.Read())
        //            {
        //                pass = false;
        //                Helper.DisplayMessage(Helper.msgType.ERR, "No more rows to fetch.");
        //                return pass;
        //            }
        //            pass = cmp_one_row(firstRow, reader, fType);
        //            break;
        //        case CommandBehavior.SingleResult:
        //        case CommandBehavior.SequentialAccess:
        //        case CommandBehavior.Default:
        //        case CommandBehavior.CloseConnection:
        //            pass = CompareData(reader, fType);
        //            break;
        //        case CommandBehavior.SchemaOnly:
        //            for (int i = 0; i < reader.FieldCount; i++)
        //            {
        //                Console.WriteLine(reader.GetName(i));// + ", " + reader.GetSchemaTable().TableName);
        //            }
        //            break;
        //        case CommandBehavior.KeyInfo:
        //            if (reader.Read())
        //                Console.WriteLine(reader[0].ToString());
        //            break;
        //        default:
        //            pass = false;
        //            Helper.DisplayMessage(Helper.msgType.ERR, "Invalid command behaviour.");
        //            break;
        //    }

        //    return pass;
        //}

        //public bool CompareData(EsgyndbDataReader reader, Helper.fetchType fType)
        //{
        //    bool pass = true;
        //    int i = 1;
        //    foreach (ArrayList aRow in this.tableInfo)
        //    {
        //        if (!reader.Read())
        //        {
        //            pass = false;
        //            Helper.DisplayMessage(Helper.msgType.ERR, "No more rows to fetch.");
        //            break;
        //        }

        //        Helper.DisplayMessage(Helper.msgType.NONE, "Start checking row " + i++);
        //        pass = cmp_one_row(aRow, reader, fType);
        //    }
        //    if (reader.Read())
        //    {
        //        pass = false;
        //        Helper.DisplayMessage(Helper.msgType.ERR, "Number of rows mismatched.");
        //    }

        //    return pass;
        //}

        //private bool cmp_one_row(ArrayList aRow, EsgyndbDataReader reader, Helper.fetchType fType)
        //{
        //    bool pass = true;
        //    if (aRow.Count != reader.FieldCount)
        //    {
        //        pass = false;
        //        Helper.DisplayMessage(Helper.msgType.ERR, "Number of columns mismatched.");
        //        return pass;
        //    }

        //    for (int i = 0; i < aRow.Count; i++)
        //    {
        //       // Helper.DisplayMessage(Helper.msgType.NONE, "Checking column " + (i + 1));// + " DBNull:" + reader.IsDBNull(i));
        //        string actVal = "";
        //        string expVal = ((aColumn)aRow[i]).expValue;

        //        switch (fType)
        //        {
        //            case Helper.fetchType.getString:
        //                actVal = reader.GetString(i);
        //                break;
        //            case Helper.fetchType.getChars:
        //                long charsize = 0;
        //                char[] charbuff = new char[2048];
        //                switch (i + 1)
        //                {
        //                    case 1:
        //                    case 3:
        //                    case 5:
        //                        charsize = reader.GetChars(i, 0, charbuff, 0, charbuff.Length);
        //                        actVal = new string(charbuff, 0, Convert.ToInt32(charsize));
        //                        break;
        //                    case 2:
        //                    case 4:
        //                    case 6:
        //                    case 7:
        //                    case 8:
        //                        charsize = reader.GetChars(i, 0, charbuff, 0, charbuff.Length);
        //                        actVal = new string(charbuff, 0, Convert.ToInt32(charsize));
        //                        break;
        //                    case 23:
        //                    case 25:
        //                        expVal = Convert.ToDateTime(expVal).ToString();
        //                        actVal = reader[i].ToString();
        //                        break;
        //                    default:
        //                        actVal = reader[i].ToString();
        //                        break;
        //                }
        //                break;
        //            case Helper.fetchType.getBytes:
        //                long bytesize = 0;
        //                byte[] bytebuff = new byte[2048];
        //                switch (i + 1)
        //                {
        //                    case 1:
        //                    case 3:
        //                    case 5:
        //                        bytesize = reader.GetBytes(i, 0, bytebuff, 0, bytebuff.Length);
        //                        actVal = Encoding.ASCII.GetString(bytebuff, 0, Convert.ToInt32(bytesize));
        //                        break;
        //                    case 2:
        //                    case 4:
        //                    case 6:
        //                    case 7:
        //                    case 8:
        //                        bytesize = reader.GetBytes(i, 0, bytebuff, 0, bytebuff.Length);
        //                        actVal = Encoding.Unicode.GetString(bytebuff, 0, Convert.ToInt32(bytesize));
        //                        break;
        //                    case 23:
        //                    case 25:
        //                        expVal = Convert.ToDateTime(expVal).ToString();
        //                        actVal = reader[i].ToString();
        //                        break;
        //                    default:
        //                        actVal = reader[i].ToString();
        //                        break;
        //                }
        //                break;
        //            case Helper.fetchType.Default:
        //                actVal = reader[i].ToString();
        //                switch (i + 1)
        //                {
        //                    case 23:
        //                    case 25:
        //                        expVal = Convert.ToDateTime(expVal).ToString();
        //                        break;
        //                    default:
        //                        break;
        //                }
        //                break;
        //            default:
        //                break;
        //        }

        //        if (!expVal.Equals(actVal))
        //        {
        //            pass = false;
        //            Helper.DisplayMessage(Helper.msgType.ERR, "expect: ==" + expVal + "==,\r\n          actual: ==" + actVal + "== at column " + (i + 1));
        //        }
        //    }
        //    Helper.DisplayMessage(Helper.msgType.NONE, "=================================================================================");

        //    return pass;
        //}

        //public void HPDbErrorCollector(EsgyndbException oe)
        //{
        //    Console.WriteLine("============================================================");
        //    Console.WriteLine("ErrorCode: " + oe.ErrorCode);
        //    foreach (EsgyndbError oErr in oe.Errors)
        //    {
        //        Console.WriteLine("Message: " + oErr.Message);
        //        Console.WriteLine("Native error: " + oErr.NativeError);
        //        Console.WriteLine("SQLState: " + oErr.SQLState);
        //        Console.WriteLine("Source: " + oErr.Source);
        //    }
        //    Console.WriteLine("============================================================");
        //}

        //public void modifyRows()
        //{
        //    ArrayList item;
        //    aColumn aCol;

        //    item = (ArrayList)this.tableInfo[9].Clone();
        //    aCol = (aColumn)item[18];
        //    aCol.expValue = "10000";
        //    item[18] = aCol;
        //    this.tableInfo.Add(item);

        //    item = (ArrayList)this.tableInfo[9].Clone();
        //    aCol = (aColumn)item[18];
        //    aCol.expValue = "10001";
        //    item[18] = aCol;
        //    this.tableInfo.Add(item);

        //    item = (ArrayList)this.tableInfo[9].Clone();
        //    aCol = (aColumn)item[18];
        //    aCol.expValue = "10002";
        //    item[18] = aCol;
        //    this.tableInfo.Add(item);
        //}

        //public void modifyDataOnRows(Helper.injections injection, List<int> rowToModify)
        //{
        //    switch (injection)
        //    {
        //        case Helper.injections.NO_ERRORS:
        //            foreach (int i in rowToModify)
        //            {
        //                aColumn aCol = (aColumn)this.tableInfo[i][16];
        //                aCol.expValue = Convert.ToString(Convert.ToInt32(aCol.expValue) + 1);
        //                this.tableInfo[i][16] = aCol;
        //            }
        //            break;
        //        case Helper.injections.DUPLICATEKEY:
        //            break;
        //        case Helper.injections.UNIQUECONST:
        //            break;
        //        case Helper.injections.DUPLICATEROW:
        //            break;
        //        case Helper.injections.STRINGOVERFLOW:
        //            foreach (int i in rowToModify)
        //            {
        //                aColumn aCol = (aColumn)this.tableInfo[i][0];
        //                aCol.expValue = this.data_bank_iso[this.data_bank_iso.Length - 1].Substring(0, aCol.size);
        //                this.tableInfo[i][0] = aCol;
        //                aCol = (aColumn)this.tableInfo[i][1];
        //                aCol.expValue = this.data_bank_charset[this.data_bank_charset.Length - 1].Substring(0, aCol.size);
        //                this.tableInfo[i][1] = aCol;
        //            }
        //            break;
        //        case Helper.injections.NUMERICOVERFLOW:
        //            break;
        //        case Helper.injections.NEGATIVEVALUE:
        //            break;
        //        case Helper.injections.NULLVALUE:
        //            break;
        //        case Helper.injections.ERR_PER_ROW:
        //            break;
        //        case Helper.injections.FULL_ERRORS:
        //            break;
        //        case Helper.injections.DRIVER_ALL_BAD_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_ALL_BAD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_ALL_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_BAD_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_BAD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.SERVER_ALL_BAD_MULCOL:
        //            break;
        //        case Helper.injections.SERVER_GOOD_BAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERBAD_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERBAD_SERVERBAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_DRIVERBAD_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_DRIVERBAD_SERVERBAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        default:
        //            break;
        //    }
        //}

        //public void modifyErrorOnRows(Helper.injections injection, List<int> rowToModify)
        //{
        //    switch (injection)
        //    {
        //        case Helper.injections.NO_ERRORS:
        //            break;
        //        case Helper.injections.DUPLICATEKEY:
        //            break;
        //        case Helper.injections.UNIQUECONST:
        //            break;
        //        case Helper.injections.DUPLICATEROW:
        //            break;
        //        case Helper.injections.STRINGOVERFLOW:
        //            foreach (int i in rowToModify)
        //            {
        //                aColumn aCol = (aColumn)this.tableInfo[i][0];
        //                aCol.expectedError = "ERROR [01004] [HP][HP Esgyndb Driver] Data truncated. Incorrect Format or Data. Row: 1 Column: 1";
        //                this.tableInfo[i][0] = aCol;
        //                aCol = (aColumn)this.tableInfo[i][1];
        //                aCol.expectedError = "ERROR [01004] [HP][HP Esgyndb Driver] Data truncated. Incorrect Format or Data. Row: 1 Column: 2";
        //                this.tableInfo[i][1] = aCol;
        //            }
        //            break;
        //        case Helper.injections.NUMERICOVERFLOW:
        //            break;
        //        case Helper.injections.NEGATIVEVALUE:
        //            break;
        //        case Helper.injections.NULLVALUE:
        //            foreach (int i in rowToModify)
        //            {
        //                aColumn aCol = (aColumn)this.tableInfo[i][3];
        //                aCol.expectedError = "ERROR [23000] [HP][HP Esgyndb Driver] General error. Null Value in a non nullable column. Row: 1 Column: 4";
        //                this.tableInfo[i][3] = aCol;
        //            }
        //            break;
        //        case Helper.injections.ERR_PER_ROW:
        //            break;
        //        case Helper.injections.FULL_ERRORS:
        //            break;
        //        case Helper.injections.DRIVER_ALL_BAD_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_ALL_BAD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_ALL_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_BAD_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_BAD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.DRIVER_GOOD_WARNING_MULCOL:
        //            break;
        //        case Helper.injections.SERVER_ALL_BAD_MULCOL:
        //            break;
        //        case Helper.injections.SERVER_GOOD_BAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERBAD_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERBAD_SERVERBAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_DRIVERBAD_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_DRIVERBAD_SERVERBAD_MULCOL:
        //            break;
        //        case Helper.injections.MIXED_DRIVERWARNING_SERVERBAD_GOOD_MULCOL:
        //            break;
        //        default:
        //            break;
        //    }
        //}

        private string spacePadding(int count)
        {
            string str = "";
            for (int i = 0; i < count; i++) str += " ";

            return str;
        }

        //public void Passed()
        //{
        //    this.TestPassed++;
        //}

        //public void Failed()
        //{
        //    this.TestFailed++;
        //    this.debugError.Add(Helper.ReportError("Test failed at:"));
        //}

        //public void StartTest(string msg)
        //{
        //    Helper.DisplayMessage(Helper.msgType.NONE,
        //        "===================================================================");
        //    Helper.DisplayMessage(Helper.msgType.NONE, msg);
        //    Helper.DisplayMessage(Helper.msgType.NONE,
        //        "===================================================================");
        //    this.TotalTest++;
        //}

        //public void PassedWithMessage(string mgs)
        //{
        //    Passed();
        //    Console.WriteLine(mgs);
        //}

        //public void FailedWithMessage(string mgs)
        //{
        //    Failed();
        //    Console.WriteLine("***ERROR: " + mgs);
        //}

        //public void TestReport()
        //{
        //    Console.WriteLine("\r\n********************** TEST RESULT ***************************");
        //    Console.WriteLine(
        //            "Test Plan = 165" +
        //            "\r\nTest Run  = " + this.TotalTest +
        //            "\r\nTest Pass = " + this.TestPassed +
        //            "\r\nTest Fail = " + this.TestFailed);
        //    int i = 1;
        //    foreach (string str in this.debugError)
        //    {
        //        Console.WriteLine(i.ToString() + ". " + str);
        //    }
        //    Console.WriteLine("**************************************************************");
        //}

    }
}
