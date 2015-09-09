using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Net;
using NUnit.Framework;
using Esgyndb.Data;
using System.Configuration;
using System.Diagnostics;
using System.Threading;

namespace TrafAdoTest
{
    [TestFixture]
    public class DoubleBufferingAndCompressionTest
    {
        private string server = ConfigurationManager.AppSettings["HostName"];
        private string connectionString;
        private EsgyndbCommand cmd;
        private EsgyndbConnection conn;

        [SetUp]
        public void SetUp()
        {
            /*
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            conn = new EsgyndbConnection(connectionString);
            conn.Open();

            cmd = conn.CreateCommand();

            //create test table
            cmd.CommandText = "CREATE TABLE TESTDBC (C1 INT NOT NULL, C2 CHAR(15), C3 NUMERIC(4,2), C4 VARCHAR(120), PRIMARY KEY(C1))";
            //Console.WriteLine(cmd.CommandText);
            cmd.ExecuteNonQuery();
             * */
        }

        [TearDown]
        public void TearDown()
        {
            /*
            //drop test table
            cmd.CommandText = "DROP TABLE TESTDBC";
            cmd.ExecuteNonQuery();
            

            cmd.Dispose();
            conn.Close();
             * */
        }

        [Test]
        public void TestWithNoneEnabled()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            conn = new EsgyndbConnection(connectionString);
            conn.Open();

            cmd = conn.CreateCommand();

            try
            {
                cmd.CommandText = "DROP TABLE TESTDBC";
                cmd.ExecuteNonQuery();
            }
            catch { }


            //create test table
            cmd.CommandText = "CREATE TABLE TESTDBC (C1 INT NOT NULL, C2 CHAR(15), C3 NUMERIC(4,2), C4 VARCHAR(120), PRIMARY KEY(C1))";
            //Console.WriteLine(cmd.CommandText);
            cmd.ExecuteNonQuery();

            //generate data, insert it into table and dump into file1
            //fetch data, dump it into file2
            //compare file1 and file2

            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from TESTDBC order by C1";

            EsgyndbDataAdapter adpw = new EsgyndbDataAdapter();
            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(adpw);

            adpw.SelectCommand = selectCmd; 
            adpw.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpw.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpw.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            //DataSet dsw = new DataSet();
            DataTable dtw = new DataTable("data1");

            adpw.Fill(dtw);

            //insert rows

            string[] datachar = 
            {
                "test test      ", 
                "123456789      ", 
                "test           ", 
                "12345          ", 
                "test test test ", 
                "123456789ABCDEF", 
                "testing        ", 
                "12345^%$#$@#   ", 
                "ABCDEFG12343567", 
                "^%$#$@#(*&(&   "
            };

            string[] datavarchar = 
            {
                "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde ", 
                "123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     ", 
                "test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12", 
                "12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   ", 
                "test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF", 
                "testing        testing        testing        testing        testing        testing        testing        testing        ", 
                "12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567", 
                "^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   "
            };

            int numrows = 1;

            Stopwatch sw = new Stopwatch();

            sw.Start();
            while (numrows <= 2000)
            {
                //dsw.Tables[0].Rows.Add(new object[] { numrows, "test test", 3.45, "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890" });
                dtw.Rows.Add(new object[] { numrows, datachar[numrows % 10], 3.45, datavarchar[numrows % 10] });
                numrows++;
            }
            
            //send values back to Esgyndb
            adpw.Update(dtw);
            sw.Stop();
            long elapsed1 = sw.ElapsedMilliseconds;
            Console.WriteLine("INSERT OF 2000 ROWS TOOK: {0}", elapsed1);

            EsgyndbDataAdapter adpr = new EsgyndbDataAdapter();
            adpr.SelectCommand = selectCmd;
            adpr.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpr.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpr.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            DataTable dtr = new DataTable("data2");

            sw.Reset();

            sw.Start();
            //select the data
            adpr.Fill(dtr);
            sw.Stop();

            long elapsed2 = sw.ElapsedMilliseconds;
            Console.WriteLine("SELECT OF 2000 ROWS TOOK: {0}", elapsed2);

            //compare dtw and dtr
            
            //ANY WAY TO COMPARE TWO XML FILES?
            dtw.WriteXml("data1NONE.log");
            dtr.WriteXml("data2NONE.log");
            int r = 0;
 
            //IF NOTHING ELSE WORKS, COMPARE EVERY ROW/COLUMN IN THE TWO DATATABLES.

            sw.Reset();
            sw.Start();
            foreach (DataRow row in dtw.Rows)
            {
                int c = 0;
                foreach (DataColumn column in dtw.Columns)
                {
                    Console.WriteLine("VALUE 1: " + row[column] + " AND VALUE 2: " + dtr.Rows[r][c]);

                    //UNCOMMENT IF NEEDED FOR DEBUGGING
                    //Assert.AreEqual(row[column], dtr.Rows[r][c]);
                    c++;
                }
                r++;
            }
            sw.Stop();

            long elapsed3 = sw.ElapsedMilliseconds;
            Console.WriteLine("DATA COMPARISON TOOK: {0}", elapsed3);

            //drop test table
            cmd.CommandText = "DROP TABLE TESTDBC";
            cmd.ExecuteNonQuery();


            cmd.Dispose();
            conn.Close();
        }

        [Test]
        public void TestWithDoubleBufferingEnabled()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            connectionString += "doublebuffering=true;";
            conn = new EsgyndbConnection(connectionString);
            conn.Open();

            cmd = conn.CreateCommand();

            try
            {
                cmd.CommandText = "DROP TABLE TESTDBC";
                cmd.ExecuteNonQuery();
            }
            catch { }

            //create test table
            cmd.CommandText = "CREATE TABLE TESTDBC (C1 INT NOT NULL, C2 CHAR(15), C3 NUMERIC(4,2), C4 VARCHAR(120), PRIMARY KEY(C1))";
            //Console.WriteLine(cmd.CommandText);
            cmd.ExecuteNonQuery();

            //generate data, insert it into table and dump into file1
            //fetch data, dump it into file2
            //compare file1 and file2

            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from TESTDBC order by C1";

            EsgyndbDataAdapter adpw = new EsgyndbDataAdapter();
            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(adpw);

            adpw.SelectCommand = selectCmd;
            adpw.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpw.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpw.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            //DataSet dsw = new DataSet();
            DataTable dtw = new DataTable("data1");

            adpw.Fill(dtw);

            //insert rows

            string[] datachar = 
            {
                "test test      ", 
                "123456789      ", 
                "test           ", 
                "12345          ", 
                "test test test ", 
                "123456789ABCDEF", 
                "testing        ", 
                "12345^%$#$@#   ", 
                "ABCDEFG12343567", 
                "^%$#$@#(*&(&   "
            };

            string[] datavarchar = 
            {
                "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde ", 
                "123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     ", 
                "test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12", 
                "12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   ", 
                "test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF", 
                "testing        testing        testing        testing        testing        testing        testing        testing        ", 
                "12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567", 
                "^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   "
            };

            int numrows = 1;

            Stopwatch sw = new Stopwatch();

            sw.Start();
            while (numrows <= 2000)
            {
                //dsw.Tables[0].Rows.Add(new object[] { numrows, "test test", 3.45, "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890" });
                dtw.Rows.Add(new object[] { numrows, datachar[numrows % 10], 3.45, datavarchar[numrows % 10] });
                numrows++;
            }

            //send values back to Esgyndb
            adpw.Update(dtw);
            sw.Stop();
            long elapsed1 = sw.ElapsedMilliseconds;
            Console.WriteLine("INSERT OF 2000 ROWS TOOK: {0}", elapsed1);

            EsgyndbDataAdapter adpr = new EsgyndbDataAdapter();
            adpr.SelectCommand = selectCmd;
            adpr.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpr.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpr.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            DataTable dtr = new DataTable("data2");

            sw.Reset();

            sw.Start();
            //select the data
            adpr.Fill(dtr);
            sw.Stop();

            long elapsed2 = sw.ElapsedMilliseconds;
            Console.WriteLine("SELECT OF 2000 ROWS TOOK: {0}", elapsed2);

            //compare dtw and dtr

            //ANY WAY TO COMPARE TWO XML FILES?
            dtw.WriteXml("data1DBUF.log");
            dtr.WriteXml("data2DBUF.log");
            int r = 0;

            //IF NOTHING ELSE WORKS, COMPARE EVERY ROW/COLUMN IN THE TWO DATATABLES.

            sw.Reset();
            sw.Start();
            foreach (DataRow row in dtw.Rows)
            {
                int c = 0;
                foreach (DataColumn column in dtw.Columns)
                {
                    //UNCOMMENT IF NEEDED FOR DEBUGGING
                    //Console.WriteLine("VALUE 1: " + row[column] + " AND VALUE 2: " + dtr.Rows[r][c]);
                    Assert.AreEqual(row[column], dtr.Rows[r][c]);
                    c++;
                }
                r++;
            }
            sw.Stop();

            long elapsed3 = sw.ElapsedMilliseconds;
            Console.WriteLine("DATA COMPARISON TOOK: {0}", elapsed3);

            //drop test table
            cmd.CommandText = "DROP TABLE TESTDBC";
            cmd.ExecuteNonQuery();


            cmd.Dispose();
            conn.Close();
        }

        [Test]
        public void TestWithCompressionEnabled()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            connectionString += "compression=true;";
            conn = new EsgyndbConnection(connectionString);
            conn.Open();

            cmd = conn.CreateCommand();

            try
            {
                cmd.CommandText = "DROP TABLE TESTDBC";
                cmd.ExecuteNonQuery();
            }
            catch { }

            //create test table
            cmd.CommandText = "CREATE TABLE TESTDBC (C1 INT NOT NULL, C2 CHAR(15), C3 NUMERIC(4,2), C4 VARCHAR(120), PRIMARY KEY(C1))";
            //Console.WriteLine(cmd.CommandText);
            cmd.ExecuteNonQuery();

            //generate data, insert it into table and dump into file1
            //fetch data, dump it into file2
            //compare file1 and file2

            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from TESTDBC order by C1";

            EsgyndbDataAdapter adpw = new EsgyndbDataAdapter();
            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(adpw);

            adpw.SelectCommand = selectCmd;
            adpw.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpw.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpw.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            //DataSet dsw = new DataSet();
            DataTable dtw = new DataTable("data1");

            adpw.Fill(dtw);

            //insert rows

            string[] datachar = 
            {
                "test test      ", 
                "123456789      ", 
                "test           ", 
                "12345          ", 
                "test test test ", 
                "123456789ABCDEF", 
                "testing        ", 
                "12345^%$#$@#   ", 
                "ABCDEFG12343567", 
                "^%$#$@#(*&(&   "
            };

            string[] datavarchar = 
            {
                "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde ", 
                "123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     ", 
                "test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12", 
                "12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   ", 
                "test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF", 
                "testing        testing        testing        testing        testing        testing        testing        testing        ", 
                "12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567", 
                "^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   "
            };

            int numrows = 1;

            Stopwatch sw = new Stopwatch();

            sw.Start();
            while (numrows <= 2000)
            {
                //dsw.Tables[0].Rows.Add(new object[] { numrows, "test test", 3.45, "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890" });
                dtw.Rows.Add(new object[] { numrows, datachar[numrows % 10], 3.45, datavarchar[numrows % 10] });
                numrows++;
            }

            //send values back to Esgyndb
            adpw.Update(dtw);
            sw.Stop();
            long elapsed1 = sw.ElapsedMilliseconds;
            Console.WriteLine("INSERT OF 2000 ROWS TOOK: {0}", elapsed1);

            EsgyndbDataAdapter adpr = new EsgyndbDataAdapter();
            adpr.SelectCommand = selectCmd;
            adpr.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpr.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpr.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            DataTable dtr = new DataTable("data2");

            sw.Reset();

            sw.Start();
            //select the data
            adpr.Fill(dtr);
            sw.Stop();

            long elapsed2 = sw.ElapsedMilliseconds;
            Console.WriteLine("SELECT OF 2000 ROWS TOOK: {0}", elapsed2);

            //compare dtw and dtr

            //ANY WAY TO COMPARE TWO XML FILES?
            dtw.WriteXml("data1COMP.log");
            dtr.WriteXml("data2COMP.log");
            int r = 0;

            //IF NOTHING ELSE WORKS, COMPARE EVERY ROW/COLUMN IN THE TWO DATATABLES.

            sw.Reset();
            sw.Start();
            foreach (DataRow row in dtw.Rows)
            {
                int c = 0;
                foreach (DataColumn column in dtw.Columns)
                {
                    //UNCOMMENT IF NEEDED FOR DEBUGGING
                    //Console.WriteLine("VALUE 1: " + row[column] + " AND VALUE 2: " + dtr.Rows[r][c]);
                    Assert.AreEqual(row[column], dtr.Rows[r][c]);
                    c++;
                }
                r++;
            }
            sw.Stop();

            long elapsed3 = sw.ElapsedMilliseconds;
            Console.WriteLine("DATA COMPARISON TOOK: {0}", elapsed3);

            //drop test table
            cmd.CommandText = "DROP TABLE TESTDBC";
            cmd.ExecuteNonQuery();


            cmd.Dispose();
            conn.Close();
        }

        [Test]
        public void TestWithBothEnabled()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            connectionString += "compression=true;doublebuffering=true;";
            conn = new EsgyndbConnection(connectionString);
            conn.Open();

            cmd = conn.CreateCommand();

            try
            {
                cmd.CommandText = "DROP TABLE TESTDBC";
                cmd.ExecuteNonQuery();
            }
            catch { }

            //create test table
            cmd.CommandText = "CREATE TABLE TESTDBC (C1 INT NOT NULL, C2 CHAR(15), C3 NUMERIC(4,2), C4 VARCHAR(120), PRIMARY KEY(C1))";
            //Console.WriteLine(cmd.CommandText);
            cmd.ExecuteNonQuery();

            //generate data, insert it into table and dump into file1
            //fetch data, dump it into file2
            //compare file1 and file2

            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from TESTDBC order by C1";

            EsgyndbDataAdapter adpw = new EsgyndbDataAdapter();
            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(adpw);

            adpw.SelectCommand = selectCmd;
            adpw.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpw.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpw.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            //DataSet dsw = new DataSet();
            DataTable dtw = new DataTable("data1");

            adpw.Fill(dtw);

            //insert rows

            string[] datachar = 
            {
                "test test      ", 
                "123456789      ", 
                "test           ", 
                "12345          ", 
                "test test test ", 
                "123456789ABCDEF", 
                "testing        ", 
                "12345^%$#$@#   ", 
                "ABCDEFG12343567", 
                "^%$#$@#(*&(&   "
            };

            string[] datavarchar = 
            {
                "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890 abcde ", 
                "123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     123456789     test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ     ", 
                "test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12test           test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  12", 
                "12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   12345test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  123456789   ", 
                "test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   test test test 12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF123456789ABCDEF", 
                "testing        testing        testing        testing        testing        testing        testing        testing        ", 
                "12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   12345^%$#$@#   ", 
                "ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567ABCDEFG12343567", 
                "^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   ^%$#$@#(*&(&   "
            };

            int numrows = 1;

            Stopwatch sw = new Stopwatch();

            sw.Start();
            while (numrows <= 2000)
            {
                //dsw.Tables[0].Rows.Add(new object[] { numrows, "test test", 3.45, "test test test ABCDEFGHIJKLMNOPQRSTUVWXYZ  1234567890" });
                dtw.Rows.Add(new object[] { numrows, datachar[numrows % 10], 3.45, datavarchar[numrows % 10] });
                numrows++;
            }

            //send values back to Esgyndb
            adpw.Update(dtw);
            sw.Stop();
            long elapsed1 = sw.ElapsedMilliseconds;
            Console.WriteLine("INSERT OF 2000 ROWS TOOK: {0}", elapsed1);

            EsgyndbDataAdapter adpr = new EsgyndbDataAdapter();
            adpr.SelectCommand = selectCmd;
            adpr.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adpr.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adpr.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            DataTable dtr = new DataTable("data2");

            sw.Reset();

            sw.Start();
            //select the data
            adpr.Fill(dtr);
            sw.Stop();

            long elapsed2 = sw.ElapsedMilliseconds;
            Console.WriteLine("SELECT OF 2000 ROWS TOOK: {0}", elapsed2);

            //compare dtw and dtr

            //ANY WAY TO COMPARE TWO XML FILES?
            dtw.WriteXml("data1BOTH.log");
            dtr.WriteXml("data2BOTH.log");
            int r = 0;

            //IF NOTHING ELSE WORKS, COMPARE EVERY ROW/COLUMN IN THE TWO DATATABLES.

            sw.Reset();
            sw.Start();
            foreach (DataRow row in dtw.Rows)
            {
                int c = 0;
                foreach (DataColumn column in dtw.Columns)
                {
                    //UNCOMMENT IF NEEDED FOR DEBUGGING
                    //Console.WriteLine("VALUE 1: " + row[column] + " AND VALUE 2: " + dtr.Rows[r][c]);
                    Assert.AreEqual(row[column], dtr.Rows[r][c]);
                    c++;
                }
                r++;
            }
            sw.Stop();

            long elapsed3 = sw.ElapsedMilliseconds;
            Console.WriteLine("DATA COMPARISON TOOK: {0}", elapsed3);

            //drop test table
            cmd.CommandText = "DROP TABLE TESTDBC";
            cmd.ExecuteNonQuery();


            cmd.Dispose();
            conn.Close();
        }
    }
}
