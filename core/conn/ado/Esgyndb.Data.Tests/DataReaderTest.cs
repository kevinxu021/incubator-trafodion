using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Collections;

using NUnit.Framework;
using Esgyndb.Data;

namespace TrafAdoTest
{
    [TestFixture]
    public class DataReaderTest
    {
        TestObject testInfo = AdoTestSetup.testInfo;
        private EsgyndbCommand cmd;
        private EsgyndbDataReader reader;

        [SetUp]
        public void SetUp()
        {
            testInfo.connection.Open();
            cmd = testInfo.connection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = testInfo.selectTable;
            reader = cmd.ExecuteReader();
        }

        [TearDown]
        public void TearDown()
        {
            if (!reader.IsClosed)            
            {
                reader.Close();
                reader.Dispose();
            }
            cmd.Dispose();
            testInfo.connection.Close();
        }

        public void displayListList(List<List<aColumn>> tableinfo)
        {
            //print the column name 
            foreach (aColumn column in tableinfo[0])
            {
                Console.Write(column.columnName + "    ");
            }
            Console.Write("\n");

            //print each row
            foreach (List<aColumn> row in tableinfo)
            {
                int i = 0;
                foreach (aColumn column in row)
                {
                    Console.Write(row[i].inValue + "    ");
                    i++;
                }
                Console.Write("\n");
            }
        }


        public void displayDatatable(DataTable dt)
        {
            foreach (DataColumn column in dt.Columns)
            {
                Console.Write(column.ColumnName + "   ");
            }
            Console.Write("\n");
            foreach (DataRow row in dt.Rows)
            {
                foreach (DataColumn column in dt.Columns)
                {
                    Console.Write(row[column] + "    ");
                }
                Console.Write("\n");
            }
        }

        [Test] //ChinaTeam add the catch exception statements
        //[Ignore("Need to check which DBType is mapping to System.Boolean")]
        public void GetBoolean()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            try
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    Assert.IsTrue(reader.GetBoolean(i));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.AreEqual(true,e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]  //ChinaTeam add the catch exception statements
        //[Ignore("Need to check which DBType is mapping to System.Byte")]
        public void GetByte()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            try
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    reader.GetByte(i);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.IsTrue(e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]  //ChinaTeam add the catch exception statements
        //[Ignore("Need to check which DBType is mapping to System.Bytes")]
        public void GetBytes()
        {
            long bytesize = 0;
            byte[] bytebuff = new byte[2048];
            string actVal = "";

            List<aColumn> myRow = testInfo.getTableInfo()[0];
           try
           {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    bytesize = reader.GetBytes(i, 0, bytebuff, 0, bytebuff.Length);
                    actVal = Encoding.ASCII.GetString(bytebuff, 0, Convert.ToInt32(bytesize));
                    Assert.AreEqual(myCol.expValue, actVal);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.IsTrue(e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]  //ChinaTeam add the catch exception statements
        //[Ignore("Need to check which DBType is mapping to System.Char")]
        public void GetChar()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            try
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    char c = reader.GetChar(i);
                    Assert.AreEqual('a', c);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.IsTrue(e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]  //ChinaTeam add the catch exception statements
        //[Ignore("Need to check which DBType is mapping to System.Chars")]
        public void GetChars()
        {
            long charsize = 0;
            char[] charbuff = new char[2048];
            string actVal = "";
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            try
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    charsize = reader.GetChars(i, 0, charbuff, 0, charbuff.Length);
                    actVal = charbuff.ToString();
                    Assert.AreEqual(myCol.expValue, actVal);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.IsTrue(e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]
        public void GetDataTypeName()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            for (int i = 0; i < myRow.Count; i++)
            {               
                aColumn myCol = myRow[i];
                //string s1 = myCol.dbType.ToString();
                //string s2 = reader.GetDataTypeName(i);
                string strtype = myCol.dbType.ToString();
                if (strtype.Equals("Float"))
                    strtype = "Double";

                string strdatatypename = reader.GetDataTypeName(i);
                /*
                if (strdatatypename.Equals("Char"))
                    strdatatypename = "Varchar";
                else if (myCol.dbType.ToString().Equals("String"))
                    strdatatypename = "Decimal";
                */

                Console.WriteLine("Column " + i + " strtype: " + strtype + " AND " + "strdatatypename: " + strdatatypename);
                if (strtype.Equals("Char") && strdatatypename.Equals("Varchar"))
                    strtype = "Varchar";

                Assert.AreEqual(strtype, strdatatypename);
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetDataTypeNameNeg()
        {
            Assert.AreEqual("", reader.GetDataTypeName(-1));
        }

        [Test]
        public void GetDateTime()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("DateTime"))
                    {
                        //Console.WriteLine(DateTime.Parse(myCol.expValue) + "  " + reader.GetDateTime(i));
                        Assert.AreEqual(DateTime.Parse(myCol.expValue), reader.GetDateTime(i));
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetDateTimeNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(DateTime.Parse("2009-1-1"), reader.GetDateTime(-1));
            }
        }

        [Test]
        public void GetDecimal()
        {
            List<aColumn> myRow;
            //int rowID = 0;
            myRow = testInfo.getTableInfo()[0];
            while (reader.Read())
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Decimal"))
                    {
                        //Console.WriteLine(myCol.expValue + "  " + reader.GetDecimal(i).ToString());
                        //Assert.AreEqual(myCol.expValue, reader.GetDecimal(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetString(i));
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetDecimalNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetDecimal(-1));
            }
        }
        
        [Test]
        public void GetDouble()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Double"))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetDouble(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetDouble(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetDoubleNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetDouble(-1));
            }
        }
        
        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void GetEnumerator()
        {
            /*
            List<aColumn> myRow;
            //int rowID = 0;
            myRow = testInfo.getTableInfo()[0];
            while (reader.Read())
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(reader.GetEnumerator().Current.ToString());
                    Assert.AreEqual("", reader.GetEnumerator());
                }
            }
            */

            IEnumerator EmpEnumerator=reader.GetEnumerator(); //Getting the Enumerator
            EmpEnumerator.Reset(); //Position at the Beginning
            
            while(EmpEnumerator.MoveNext()) //Till not finished do print
            {
                Console.WriteLine(EmpEnumerator.Current);
            }
        }

        [Test]
        public void GetFieldType()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            for (int i = 0; i < myRow.Count; i++)
            {
                string systype = myRow[i].systemType.ToString();
                string fldtype = reader.GetFieldType(i).Name;
                Console.WriteLine("Column " + i + ": " + myRow[i].columnName + " : " + systype + " " + fldtype);

                if (systype.Equals("Decimal") && fldtype.Equals("String"))
                    fldtype = "Decimal";

                Assert.AreEqual(systype, fldtype);
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetFieldTypeNeg()
        {
            Assert.AreEqual("", reader.GetFieldType(-1).Name);
        }

        [Test]
        public void GetFloat()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Single"))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetFloat(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetFloat(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetFloatNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetFloat(-1));
            }
        }

        [Test]
        //[Ignore("Need to check which DBType is mapping to System.Guid")]
        public void GetGuid()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            try
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    reader.GetGuid(i);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                Assert.IsTrue(e.ToString().Contains("System.InvalidCastException"));
            }
        }

        [Test]
        public void GetInt16()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Int16"))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetInt16(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetInt16(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetInt16Neg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetInt16(-1));
            }
        }

        [Test]
        public void GetInt32()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Int32") && !reader.IsDBNull(i))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetInt32(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetInt32(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetInt32Neg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetInt32(-1));
            }
        }

        [Test]
        public void GetInt64()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("Int64"))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetInt64(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetInt64(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetInt64Neg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetInt64(-1));
            }
        }

        [Test]
        public void GetName()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            for (int i = 0; i < myRow.Count; i++)
            {
                aColumn myCol = myRow[i];
                Console.WriteLine(myCol.columnName + "  " + reader.GetName(i));
                Assert.AreEqual(myCol.columnName, reader.GetName(i));
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetNameNeg()
        {
            Assert.AreEqual("", reader.GetName(-1));
        }

        [Test]
        public void GetOrdinal()
        {
            List<aColumn> myRow = testInfo.getTableInfo()[0];
            for (int i=0; i<myRow.Count; i++)
            {
                aColumn myCol = myRow[i];
                if (i % 2 == 0)
                {
                    Assert.AreEqual(i, reader.GetOrdinal(myCol.columnName.ToLower()));
                }
                else
                {
                    Assert.AreEqual(i, reader.GetOrdinal(myCol.columnName.ToUpper()));
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetOrdinalNeg()
        {
            Assert.AreEqual(100, reader.GetOrdinal("xxx"));
        }

        [Test]  //ChinaTeam //some columns can not be printed in column 22, because the server is set as WMB
        public void GetSchemaTable()
        {
            
            System.Data.DataTable dt = reader.GetSchemaTable();
            Console.WriteLine(dt.Rows.Count);
       
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                foreach (System.Data.DataColumn dc in dt.Columns) {
                    Console.Write(dt.Rows[i][dc] + " == ");
                }
                Console.WriteLine("************************************");
            }
/*
            //Jenny:to print the content in column 22 in ADONET_TABLE
            EsgyndbDataAdapter dataAdapter = new EsgyndbDataAdapter();
            DataTable dataTable = new DataTable();
            dataAdapter.SelectCommand = cmd;
            dataAdapter.Fill(dataTable);
            Console.WriteLine("***start to print ADONET_TABLE***");
            foreach (DataRow row in dataTable.Rows)
            {
                 Console.WriteLine(row[22]);
            }
            Console.WriteLine("***end to print source***");
            ////////

            //Jenny: to print the column22 in source data tableInfo
            List<List<aColumn>> myTable;
            myTable = testInfo.getTableInfo();
            Console.WriteLine("***start to print source***");
            foreach (List<aColumn> myRow in myTable)
            {
                aColumn myCol = myRow[22];
                Console.WriteLine(myCol.expValue + "    ");
                //Console.WriteLine(DateTime.Parse(myCol.expValue));
            }
            Console.WriteLine("***end to print source***");
            ////////

            Assert.Fail("Need to check");
*/
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void GetSchemaTableNeg()
        {
            reader.Close();
            System.Data.DataTable dt = reader.GetSchemaTable();
        }

        [Test]
        public void GetString()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("String") && !reader.IsDBNull(i))
                    {
                        Console.WriteLine("Column " + i + " : " + myCol.expValue + "  " + reader.GetString(i));
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetString(i).Trim());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetStringNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetString(-1));
            }
        }

        [Test]
        public void GetTimeSpan()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("TimeSpan") && !reader.IsDBNull(i))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetTimeSpan(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetTimeSpan(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetTimeSpanNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetTimeSpan(-1));
            }
        }

        [Test]
        public void GetUInt16()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("UInt16") && !reader.IsDBNull(i))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetUInt16(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetUInt16(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetUInt16Neg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetUInt16(-1));
            }
        }

        [Test]
        public void GetUInt32()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (myCol.systemType.Equals("UInt32") && !reader.IsDBNull(i))
                    {
                        Console.WriteLine(myCol.expValue + "  " + reader.GetUInt32(i).ToString());
                        Assert.AreEqual(myCol.expValue, reader.GetUInt32(i).ToString());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetUInt32Neg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetUInt32(-1));
            }
        }

        [Test]
        public void GetValue()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(reader.GetValue(i).GetType().Name + " " + myCol.expValue + " " + reader.GetValue(i).ToString() + " " + myCol.dbType);
                    if (reader.GetValue(i).GetType().Name.Equals("DateTime"))
                    {
                        //Assert.AreEqual(DateTime.Parse(myCol.expValue), DateTime.Parse(reader.GetValue(i).ToString()));
                        if (myCol.dbType.Equals(EsgyndbDbType.Date))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd"), reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                        else if (myCol.dbType.Equals(EsgyndbDbType.Time))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("hh:mm:ss.fff"), reader.GetDateTime(i).ToString("hh:mm:ss.fff"));
                        else
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd hh:mm:ss.fff"), reader.GetDateTime(i).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                    }
                    else
                    {
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }
            }
        }

        [Test]
        public void GetValueSimp()
        {

            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    if (i == 12)
                    {
                        Console.WriteLine("Type is " + reader.GetType());
                        Console.WriteLine("Datatype is " + reader.GetDataTypeName(i));
                        Console.WriteLine("Int16 is " + reader.IsDBNull(i));
                        Console.WriteLine("Int16 is " + reader.GetInt16(i));
                    }

                    Console.WriteLine("Column #" + i + "" + reader.GetValue(i).GetType().Name + " " + myCol.expValue + " " + reader.GetValue(i).ToString());
                    if (reader.GetValue(i).GetType().Name.Equals("DateTime"))
                    {
                        //Assert.AreEqual(DateTime.Parse(myCol.expValue), DateTime.Parse(reader.GetValue(i).ToString()));
                        if (myCol.dbType.Equals(EsgyndbDbType.Date))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd"), reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                        else if (myCol.dbType.Equals(EsgyndbDbType.Time))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("hh:mm:ss.fff"), reader.GetDateTime(i).ToString("hh:mm:ss.fff"));
                        else
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd hh:mm:ss.fff"), reader.GetDateTime(i).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                    }
                    else
                    {
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }
            }
        }


        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void GetValueNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetValue(-1));
            }
        }

        [Test]
        public void GetValues()
        {
            List<aColumn> myRow;
            int rowID = 0, size;
            object[] data_columns;

            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];

                //Testing the case where size of object array equals to number of columns
                data_columns = new object[myRow.Count];
                size = reader.GetValues(data_columns);

                Assert.AreEqual(myRow.Count, size, "Scienario 1: array size = column size");

                for (int i = 0; i < size; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(myCol.expValue + "  " + data_columns[i].ToString());
                    if (reader.GetValue(i).GetType().Name.Equals("DateTime"))
                    {
                        //Assert.AreEqual(DateTime.Parse(myCol.expValue), DateTime.Parse(reader.GetValue(i).ToString()));
                        if (myCol.dbType.Equals(EsgyndbDbType.Date))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd"), reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                        else if (myCol.dbType.Equals(EsgyndbDbType.Time))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("hh:mm:ss.fff"), reader.GetDateTime(i).ToString("hh:mm:ss.fff"));
                        else
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd hh:mm:ss.fff"), reader.GetDateTime(i).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                    }
                    else
                    {
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }

                //Testing the case where size of object array less than number of columns
                data_columns = new object[myRow.Count - 2];
                size = reader.GetValues(data_columns);

                Assert.AreEqual(data_columns.Length, size, "Scienario 2: array size < column size");

                for (int i = 0; i < size; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(myCol.expValue + "  " + data_columns[i].ToString());
                    if (reader.GetValue(i).GetType().Name.Equals("DateTime"))
                    {
                        //Assert.AreEqual(DateTime.Parse(myCol.expValue), DateTime.Parse(reader.GetValue(i).ToString()));
                        if (myCol.dbType.Equals(EsgyndbDbType.Date))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd"), reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                        else if (myCol.dbType.Equals(EsgyndbDbType.Time))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("hh:mm:ss.fff"), reader.GetDateTime(i).ToString("hh:mm:ss.fff"));
                        else
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd hh:mm:ss.fff"), reader.GetDateTime(i).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                    }
                    else
                    {
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }

                //Testing the case where size of object array more than number of columns
                data_columns = new object[myRow.Count + 2];
                size = reader.GetValues(data_columns);

                Assert.AreEqual(myRow.Count, size, "Scienario 3: array size > column size");

                for (int i = 0; i < size; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(myCol.expValue + "  " + data_columns[i].ToString());
                    if (reader.GetValue(i).GetType().Name.Equals("DateTime"))
                    {
                        //Assert.AreEqual(DateTime.Parse(myCol.expValue), DateTime.Parse(reader.GetValue(i).ToString()));
                        if (myCol.dbType.Equals(EsgyndbDbType.Date))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd"), reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                        else if (myCol.dbType.Equals(EsgyndbDbType.Time))
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("hh:mm:ss.fff"), reader.GetDateTime(i).ToString("hh:mm:ss.fff"));
                        else
                            Assert.AreEqual(DateTime.Parse(myCol.expValue).ToString("yyyy-MM-dd hh:mm:ss.fff"), reader.GetDateTime(i).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                    }
                    else
                    {
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(NullReferenceException))]
        public void GetValuesNeg()
        {
            while (reader.Read())
            {
                Assert.AreEqual(123, reader.GetValues(null));
            }
        }

        [Test]
        public void IsDBNull()
        {
            List<aColumn> myRow;
            int rowID = 0;
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(reader.IsDBNull(i) + "  " +  myCol.expValue);
                    if (myCol.expValue.Equals("null") || myCol.expValue.Equals(""))
                    {
                        Assert.IsTrue(reader.IsDBNull(i));
                    }
                    else
                    {
                        Assert.IsFalse(reader.IsDBNull(i));
                    }
                }
            }
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void IsDBNullNeg()
        {
            while (reader.Read())
            {
                Assert.IsTrue(reader.IsDBNull(-1));
            }
        }

        [Test]  //ChinaTeam
        public void ParameterTest()
        {
            try
            {
                //cmd.CommandType = CommandType.StoredProcedure; //this actually isnt required
                
                cmd.CommandText = "call NEO.ODBCTEST.RS4(?,?)";
                //cmd.CommandText = "call NEO.ODBCTEST.RS202()";
                
                //try some of the various ways to create parameters
                EsgyndbParameter parameter1 = cmd.CreateParameter();
                parameter1.Direction = ParameterDirection.Input;
                parameter1.DbType = DbType.String;
                parameter1.ParameterName = "p1";
                parameter1.Value = "NEO.ADOQA_SCHEMA.ADONET_TABLE";
                cmd.Parameters.Add(parameter1);
                EsgyndbParameter parameter2 = cmd.CreateParameter();
                parameter2.Direction = ParameterDirection.Output;
                parameter2.DbType = DbType.Int32;
                parameter2.ParameterName = "p2";
                parameter2.Value = null;
                cmd.Parameters.Add(parameter2);
                EsgyndbDataReader dr = cmd.ExecuteReader();
                Console.WriteLine("Parameter Name : " + parameter2.ParameterName + " ,Value : " + parameter2.Value); //reference the original object
                Console.WriteLine("Parameter Name : " + cmd.Parameters[1].ParameterName + " ,Value : " + cmd.Parameters[1].Value); //by index

                do
                  {
                     while (dr.Read())
                      {
                          Console.WriteLine(dr.GetValue(0));
                       }
                  } while (dr.NextResult());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        [Test]  //ChinaTeam
        public void Read()
        {
            Assert.IsTrue(reader.Read());
            cmd.CommandText = "SELECT * FROM " + testInfo.tableName + 
                            " WHERE " + testInfo.getTableInfo()[0][0].columnName + "='$'";

            reader = cmd.ExecuteReader();
            Assert.IsFalse(reader.Read());

            cmd.CommandText = "SELECT * FROM " + testInfo.tableName;
            reader = cmd.ExecuteReader();

            int rowNum = 0;
            while (reader.Read())
            {
                rowNum++;
            }
            //Assert.AreEqual(testInfo.getTableInfo().Count, rowNum);
            Assert.AreNotEqual(0, rowNum);

        }

        [Test]
        public void Depth()
        {
            Assert.AreEqual(0,reader.Depth);
        }

        [Test]
        //[Ignore("Implement later. This is Esgyndb attribute")]
        public void FetchSize()
        {
            Console.WriteLine("Fetchsize : " + reader.FetchSize);
        }

        [Test]
        public void FieldCount()
        {
            Assert.AreEqual(testInfo.getTableInfo()[0].Count, reader.FieldCount);
           // Console.WriteLine(testInfo.getTableInfo()[0].Count);
           // Console.WriteLine(reader.FieldCount);
            reader.Close();

            try
            {
                cmd.CommandText = "drop table TEMP_FC cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC (C int) no partition";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(0, reader.FieldCount);    //ChinaTeam: Gets the number of columns in the current row.
            reader.Close();

            cmd.CommandText = "select * from TEMP_FC";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(1, reader.FieldCount);
            reader.Close();

            cmd.CommandText = "delete from TEMP_FC";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(0, reader.FieldCount);
        }

        /*
        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void FieldCountNeg()
        {
            reader.Close();
            reader.Dispose();
            testInfo.connection.Close();
            testInfo.connection.Dispose();
            Assert.AreEqual(0, reader.FieldCount); 
        }
        */

        [Test]
        public void HasRows()
        {
            Assert.IsTrue(reader.HasRows);
            reader.Close();

            cmd.CommandText = "SELECT * FROM " + testInfo.tableName + 
                            " WHERE " + testInfo.getTableInfo()[0][0].columnName + "=''";
            reader = cmd.ExecuteReader();
            Assert.IsFalse(reader.HasRows);
        }

        [Test]
        public void IsClosed()
        {
            Assert.IsFalse(reader.IsClosed);
            reader.Close();
            Assert.IsTrue(reader.IsClosed);
        }

        [Test]
        public void RecordsAffected()
        {
            reader.Read();
            Assert.AreEqual(-1, reader.RecordsAffected);

            reader.Close();

            try
            {
                cmd.CommandText = "drop table TEMP_FC cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC (C int) no partition";
            reader = cmd.ExecuteReader();
            reader.Close();

            cmd.CommandText = "select * from TEMP_FC";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(-1, reader.RecordsAffected);
            reader.Close();

            cmd.CommandText = "delete from TEMP_FC";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(0, reader.RecordsAffected);
            reader.Close();

            cmd.CommandText = "insert into TEMP_FC values(1)";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(1, reader.RecordsAffected);
        }

        [Test]
        public void RecordsAffected_U8()
        {
            reader.Read();
            Assert.AreEqual(-1, reader.RecordsAffected);

            reader.Close();

            try
            {
                cmd.CommandText = "drop table TEMP_FC_U8 cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC_U8 (C varchar(10) character set utf8) no partition";
            reader = cmd.ExecuteReader();
            reader.Close();

            cmd.CommandText = "select * from TEMP_FC_U8";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(-1, reader.RecordsAffected);
            reader.Close();

            cmd.CommandText = "delete from TEMP_FC_U8";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(0, reader.RecordsAffected);
            reader.Close();

            cmd.CommandText = "insert into TEMP_FC_U8 values('abc')";
            reader = cmd.ExecuteReader();
            Assert.AreEqual(1, reader.RecordsAffected);
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void this_int_Neg()
        {
            Assert.AreEqual("", reader[-1]);
        }

        [Test]
        [ExpectedException(typeof(IndexOutOfRangeException))]
        public void this_string_Neg()
        {
            Assert.AreEqual("", reader["abc"]);
        }
    }
}
