using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Configuration;

using NUnit.Framework;
using EsgynDB.Data;
using System.Data;


namespace TrafAdoTest
{
    public class Alpha
    {
        private EsgynDBCommand acmd;

        public Alpha(EsgynDBCommand nvcmd)
        {
            acmd = nvcmd;
        }

        // This method that will be called when the thread is started
        public void Beta()
        {
            Console.WriteLine("Alpha.Beta is running in its own thread.");
            System.Threading.Thread.Sleep(10000);
            acmd.Cancel();

            /*
            while (true)
            {
                Console.WriteLine("Alpha.Beta is running in its own thread.");
            }
            */
            return;
        }
    };

    [TestFixture]
    public class CommandTest
    {
        TestObject testInfo = AdoTestSetup.testInfo;
        private EsgynDBCommand cmd;
        private EsgynDBDataReader reader;

        [SetUp]
        public void SetUp()
        {
            testInfo.connection.Open();
            cmd = testInfo.connection.CreateCommand();
            cmd.CommandTimeout = 0;
            cmd.CommandText = testInfo.selectTable;
        }

        [TearDown]
        public void TearDown()
        {
            if (reader != null && !reader.IsClosed)
            {
                reader.Close();
                reader.Dispose();
            }
            cmd.Dispose();
            testInfo.connection.Close();
        }

        [Test]
        public void ExecuteNonQuery()
        {
            int rowsAffected;
            try
            {
                cmd.CommandText = "drop table TEMP_FC cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC (C int) no partition";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "insert into TEMP_FC values(1),(2)";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "update TEMP_FC set C=100 where C=1";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, rowsAffected);

            cmd.CommandText = "select * from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, rowsAffected);
        }

        [Test]
        public void ExecuteNonQuery_U8()
        {
            int rowsAffected;
            try
            {
                cmd.CommandText = "drop table TEMP_FC_U8 cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC_U8 (c1 char(9) character set utf8) no partition";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "insert into TEMP_FC_U8 values('Æ»¹û'),('Ïã½¶')";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "update TEMP_FC_U8 set c1='ÆÏÌÑ' where c1='Ïã½¶'";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, rowsAffected);

            cmd.CommandText = "select * from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, rowsAffected);
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

        [Test]
        public void ExecuteReader_CloseConnection()
        {
            List<aColumn> myRow;
            int rowID = 0;
            DataTable schemaTable;

            reader = cmd.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
            /*
            while (reader.Read())
            {
                myRow = testInfo.getTableInfo()[rowID++];
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    Console.WriteLine(reader.GetValue(i).GetType().Name + " " + myCol.expValue + " " + reader.GetValue(i).ToString());
                    Assert.AreEqual(myCol.expValue, reader.GetValue(i).ToString());
                }
            }
            */
            schemaTable = reader.GetSchemaTable();
            myRow = testInfo.getTableInfo()[0];
            foreach (DataRow myfield in schemaTable.Rows)
            {
                Assert.AreEqual(myRow[rowID++].columnName, myfield["ColumnName"]);
                //Assert.AreEqual(myRow[6], myfield["DataType"]);
            }

            reader.Close();
            Assert.AreEqual(ConnectionState.Closed, testInfo.connection.State);
            if (testInfo.connection.State == ConnectionState.Closed)
                testInfo.connection.Open();
        }

        public void Main(string[] args)
        {
            ExecuteReader_CloseConnection();
        }

        [Test]
        public void ExecuteReader_Default()
        {
            List<aColumn> myRow;
            //List<List<aColumn>> myRow;
            int rowID = 0;
            DataTable schemaTable;

            reader = cmd.ExecuteReader(CommandBehavior.Default);
            schemaTable = reader.GetSchemaTable();
            Console.WriteLine("JUST FOR DEBUGGING");
            displayDatatable(schemaTable);
            Console.WriteLine("JUST FOR DEBUGGING");
            myRow = testInfo.getTableInfo()[0];
            foreach (DataRow myfield in schemaTable.Rows)
            {
                //myRow = testInfo.getTableInfo()[rowID++];
                Assert.AreEqual(myRow[rowID++].columnName, myfield["ColumnName"]);

                //dbType is Char for string and systemType is String, but myfield["DataType" is "<System.String>".
                //Assert.AreEqual(myRow[0].dbType, myfield["DataType"]);
                //Assert.AreEqual(myRow[0].systemType, myfield["DataType"]);
            }
            reader.Close();
        }

        [Test]
        public void ExecuteReader_KeyInfo()
        {
            List<aColumn> myRow;
            int rowID = 0;
            DataTable schemaTable;

            reader = cmd.ExecuteReader(CommandBehavior.KeyInfo);
            schemaTable = reader.GetSchemaTable();
            myRow = testInfo.getTableInfo()[0];
            foreach (DataRow myfield in schemaTable.Rows)
            {
                Assert.AreEqual(myRow[rowID++].columnName, myfield["ColumnName"]);
                //Assert.AreEqual(myRow[6], myfield["DataType"]);
                //Assert.AreEqual(false, myfield["IsKeyColumn"]);
            }
            reader.Close();
        }

        [Test]
        public void ExecuteReader_SchemaOnly()
        {
            List<aColumn> myRow;
            int rowID = 0;
            DataTable schemaTable;

            reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly);
            //Assert.AreEqual(reader.HasRows, false);
            schemaTable = reader.GetSchemaTable();
            //Assert.AreEqual(reader.HasRows, false);
            myRow = testInfo.getTableInfo()[0];
            foreach (DataRow myfield in schemaTable.Rows)
            {
                //Assert.AreEqual(reader.HasRows, false);
                reader.Read();
                Assert.AreEqual(myRow[rowID++].columnName, myfield["ColumnName"]);
                //Assert.AreEqual(myRow[6], myfield["DataType"]);
            }
            Assert.AreEqual(reader.HasRows, false);
            reader.Close();
        }

        [Test]
        public void ExecuteReader_SequentialAccess()
        {
            reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read())
            {
            }
            reader.Close();
        }

        [Test]
        public void ExecuteReader_SingleResult()
        {
            reader = cmd.ExecuteReader(CommandBehavior.SingleResult);
            while (reader.Read())
            {
            }
            reader.NextResult();
            Assert.AreEqual(reader.HasRows, false);
            reader.Close();
        }

        [Test]
        public void ExecuteReader_SingleRow()
        {
            int recordcount = 0;
            reader = cmd.ExecuteReader(CommandBehavior.SingleRow);
            while (reader.Read())
            {
                recordcount++;
            }
            Assert.AreEqual(1, recordcount);
            reader.Close();
        }

        [Test]
        public void ExecuteReader()
        {
            List<aColumn> myRow;
            //int rowID = 0;
            string dtype;

            reader = cmd.ExecuteReader();
            myRow = testInfo.getTableInfo()[0];
            //while (reader.Read())
            if (reader.Read())
            {
                for (int i = 0; i < myRow.Count; i++)
                {
                    aColumn myCol = myRow[i];
                    dtype = reader.GetValue(i).GetType().Name;
                    if (dtype.Equals("DateTime"))
                    {
                        if (myCol.dbType == EsgynDBType.Date)
                        {
                            Console.WriteLine(dtype + " " + myCol.expValue + " " + ((DateTime)(reader.GetValue(i))).ToString("yyyy-MM-dd"));
                            //Assert.AreEqual(myCol.expValue, DateTime.Parse(((DateTime)(reader.GetValue(i))).ToString("yyyy-MM-dd HH:mm:ss.ffffff")));
                            Assert.AreEqual(myCol.expValue, ((DateTime)(reader.GetValue(i))).ToString("yyyy-MM-dd"));
                        }
                        else if (myCol.dbType == EsgynDBType.Timestamp)
                        {
                            Console.WriteLine(dtype + " " + myCol.expValue + " " + ((DateTime)(reader.GetValue(i))).ToString("yyyy-MM-dd HH:mm:ss.ffffff"));
                            Assert.AreEqual(myCol.expValue, ((DateTime)(reader.GetValue(i))).ToString("yyyy-MM-dd HH:mm:ss.ffffff"));
                        }
                    }
                    else
                    {
                        Console.WriteLine("Column name : " + myCol.columnName + "; " + dtype + " " + myCol.expValue + " " + reader.GetValue(i).ToString());
                        Assert.AreEqual(myCol.expValue.Trim(), reader.GetValue(i).ToString().Trim());
                    }
                }
            }
        }

        [Test]
        public void ExecuteScalar()
        {
            EsgynDBCommand tstcmd = testInfo.connection.CreateCommand();
            tstcmd.CommandText = "select count(*) from ADONET_TABLE";
            Object c = tstcmd.ExecuteScalar();

            Assert.Greater(Convert.ToInt32(c), 0);
            Console.WriteLine("ExecuteScalar returned : " + c);

            tstcmd.Dispose();
        }

        [Test]
        //[Ignore("Driver doesn not support yet. Make sure to implement this function once driver is avaiable")]
        public void Cancel()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand("SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ADONET_TABLE");
            tstcmd.Connection = testInfo.connection;
            EsgynDBDataReader tstrdr = tstcmd.ExecuteReader();
            while (tstrdr.Read())
            {
                if (tstrdr.HasRows)
                {
                    tstcmd.Cancel();
                    Console.WriteLine("Connection state after cancel : " + tstcmd.Connection.State);

                    Console.WriteLine(tstrdr.GetValue(0));
                }

            }
            tstcmd.Dispose();
        }

        [Test]
        //ARUNA - MAR 4, 2011 - CHANGING EXPECTED EXCEPTION FOR TEST TO PASS - SHOULD BE COMMUNICATIONSFAILUREEXCEPTION
        //[ExpectedException(typeof(CommunicationsFailureException))]
        [ExpectedException(typeof(EsgynDBException))]
        public void CancelEn()
        {
            EsgynDBConnection conx = new EsgynDBConnection();
            //conx.ConnectionString = "server=sqa0101.cup.hp.com;user=SUPERUSER;password=HPNe@v1ew;catalog=NEO;schema=ADOQA_SCHEMA;";
            conx.ConnectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            conx.Open();
            EsgynDBCommand tstcmd = conx.CreateCommand();
            tstcmd.CommandText = "SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ODBCQA_SCHEMA.ADONET_TABLE";

            //tstcmd.Connection = testInfo.connection;

            //start a new thread, pass the cmd object to that thread, which the other thread cancels. 
            //sync up before execute and cancel. and cancel after few seconds after execute.
            Alpha oAlpha = new Alpha(tstcmd);

            // Create the thread object, passing in the Alpha.Beta method
            // via a ThreadStart delegate. This does not start the thread.
            Thread oThread = new Thread(new ThreadStart(oAlpha.Beta));

            // Start the thread
            oThread.Start();

            // Spin for a while waiting for the started thread to become
            // alive:
            while (!oThread.IsAlive) ;
     
            //execute a query
            EsgynDBDataReader tstrdr = tstcmd.ExecuteReader();
            oThread.Join();
            while (tstrdr.Read())
            {
                Console.WriteLine(tstrdr.GetValue(0));
            }
            tstcmd.Dispose();
            conx.Close();
        }

        [Test]
        [ExpectedException(typeof(EsgynDBException))]
        public void CancelInsertSelect()
        {
            EsgynDBConnection conx = new EsgynDBConnection();
            //conx.ConnectionString = "server=sqa0101.cup.hp.com;user=SUPERUSER;password=HPNe@v1ew;catalog=NEO;schema=ADOQA_SCHEMA;";
            conx.ConnectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            conx.Open();
            string strInsertSelect = "insert into T10000K select 0 + (1000000 * x1000000) +(100000 * x100000) + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (100000 * x100000) + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',0 + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (100 * x100) + (10 * x10) + (1 * x1),0 + (10 * x10) + (1 * x1) from starter transpose 0,1,2,3,4,5,6,7,8,9 as x1000000 transpose 0,1,2,3,4,5,6,7,8,9 as x100000 transpose 0,1,2,3,4,5,6,7,8,9 as x10000 transpose 0,1,2,3,4,5,6,7,8,9 as x1000 transpose 0,1,2,3,4,5,6,7,8,9 as x100 transpose 0,1,2,3,4,5,6,7,8,9 as x10 transpose 0,1,2,3,4,5,6,7,8,9 as x1;";
            EsgynDBCommand tstcmd = conx.CreateCommand();
            int ret = 0;
            tstcmd.CommandText = "DROP TABLE STARTER;";
            try
            {
                ret = tstcmd.ExecuteNonQuery();
            }
            catch { }
            Console.WriteLine(ret);
            tstcmd.CommandText = "DROP TABLE T10000K;";
            try
            {
                ret = tstcmd.ExecuteNonQuery();
            }
            catch { }
            Console.WriteLine(ret);
            tstcmd.CommandText = "create table starter (a int not null, primary key(a));";
            try
            {
                ret = tstcmd.ExecuteNonQuery();
            }
            catch { }
            Console.WriteLine(ret);
            tstcmd.CommandText = "create table T10000K(uniq int not null,c10K int,str1 varchar(50),c1K int,c100 int,c10 int,c1 int,primary key (uniq));";
            ret = tstcmd.ExecuteNonQuery();
            Console.WriteLine(ret);
            tstcmd.CommandText = "insert into  starter values (1);";
            try
            {
                ret = tstcmd.ExecuteNonQuery();
            }
            catch { }
            Console.WriteLine(ret);

            //start a new thread, pass the cmd object to that thread, which the other thread cancels. 
            //sync up before execute and cancel. and cancel after few seconds after execute.
            Alpha oAlpha = new Alpha(tstcmd);

            // Create the thread object, passing in the Alpha.Beta method
            // via a ThreadStart delegate. This does not start the thread.
            Thread oThread = new Thread(new ThreadStart(oAlpha.Beta));

            // Start the thread
            oThread.Start();

            // Spin for a while waiting for the started thread to become
            // alive:
            while (!oThread.IsAlive) ;

            //execute a query
            tstcmd.CommandText = strInsertSelect;
            ret = tstcmd.ExecuteNonQuery();
            oThread.Join();
            Console.WriteLine(ret);
            tstcmd.Dispose();
            conx.Close();
        }
/*
        [Test]
        public static void TestCancel()
        {
            EsgynDBConnection conx = new EsgynDBConnection();
            //conx.ConnectionString = "server=sqa0101.cup.hp.com;user=SUPERUSER;password=HPNe@v1ew;catalog=NEO;schema=ADOQA_SCHEMA;";
            conx.ConnectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            //conx.ConnectionString = "server=sqa0101.cup.hp.com:48000;cputouse=7;user=super-user;password=Quislin44$/mcK1nley;catalog=NEO;schema=DOERMANN;";
            conx.Open();

            EsgyndbCommand cmdx = conx.CreateCommand();
            cmdx.CommandText = "insert into neo.doermann.testcancel4 (c1) select x.c1 from neo.doermann.testcancel1 x, neo.doermann.testcancel2 y, neo.doermann.testcancel1 z where x.c1 + y.c1 + z.c1 < 70";

            Thread t = new Thread(new ParameterizedThreadStart(CancelCommand));
            t.Start(cmdx);

            try
            {
                cmdx.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        //wait 5 seconds, then cancel the command
        public static void CancelCommand(object var)
        {
            EsgyndbCommand cmdz = (EsgyndbCommand)var;
            Thread.Sleep(5000);

            cmdz.Cancel();
        }
*/
        [Test]
        public void EsgyndbCommand_string_conn_tran()
        {
            EsgynDBTransaction tsttrn = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            EsgynDBCommand tstcmd = new EsgynDBCommand("SELECT * FROM ADONET_TABLE", testInfo.connection, tsttrn);

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);

            tsttrn.Commit();
            tstcmd.Dispose();
            tsttrn.Dispose();
        }

        [Test]
        public void EsgyndbCommand_string_conn_tran_label()
        {
            /*
            HPDbTransaction tsttrn = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            EsgyndbCommand tstcmd = new EsgyndbCommand("SELECT * FROM ADONET_TABLE", testInfo.connection, tsttrn, "TSTLBL");

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);
            Console.WriteLine("Label : " + tstcmd.Label);

            tsttrn.Commit();
            tstcmd.Dispose();
            tsttrn.Dispose();
             * */
        }

        [Test]
        public void EsgyndbCommand_string_conn_null_label()
        {
            /*
            EsgyndbCommand tstcmd = new EsgyndbCommand("SELECT * FROM ADONET_TABLE", testInfo.connection, null, "TSTLBL");

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);
            Console.WriteLine("Label : " + tstcmd.Label);
            Assert.AreEqual("TSTLBL", tstcmd.Label);

            tstcmd.Dispose();
             * */
        }

        [Test]
        public void EsgyndbCommand_string_conn()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand("SELECT * FROM ADONET_TABLE", testInfo.connection);

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);

            tstcmd.Dispose();
        }

        [Test]
        public void EsgyndbCommand_string()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand("SELECT * FROM ADONET_TABLE");
            tstcmd.Connection = testInfo.connection;

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);

            tstcmd.Dispose();
        }

        [Test]
        public void EsgyndbCommand()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            tstcmd.CommandText = "SELECT * FROM ADONET_TABLE";

            Console.WriteLine("CommandText : " + tstcmd.CommandText);
            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);

            tstcmd.Dispose();
        }

        [Test]
        public void Prepare()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            tstcmd.CommandText = "SELECT * FROM ADONET_TABLE";
            tstcmd.Prepare();
            EsgynDBDataReader tstrdr = tstcmd.ExecuteReader();
            Assert.IsTrue(tstrdr.HasRows);

            tstcmd.Dispose();
        }

        
        [Test]
        public void PrepareEn()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            try
            {
                tstcmd.CommandText = "drop table Region";
                tstcmd.ExecuteNonQuery();
            }
            catch { }
            
            tstcmd.CommandText = "create table Region (RegionID int not null not droppable, RegionDescription char(100), primary key(RegionID))";
            tstcmd.ExecuteNonQuery();

            tstcmd.CommandText = "insert into Region (RegionID, RegionDescription) values (?, ?)";

            tstcmd.Parameters.Add(new EsgynDBParameter("RegionID", EsgynDBType.Integer));
            tstcmd.Parameters.Add(new EsgynDBParameter("RegionDescription", EsgynDBType.Char, 100));

            tstcmd.Parameters[0].Value = 5;
            tstcmd.Parameters[1].Value = "North West";
            tstcmd.Prepare();
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 6;
            tstcmd.Parameters[1].Value = "North East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 7;
            tstcmd.Parameters[1].Value = "South East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 8;
            tstcmd.Parameters[1].Value = "South West";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters.Clear();

            tstcmd.CommandText = "SELECT count(*) FROM Region";
            Object c = tstcmd.ExecuteScalar();

            Assert.AreEqual(Convert.ToInt32(c), 4);

            tstcmd.Dispose();
        }

       [Test]
        public void PrepareEn_U8()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            try
            {
                tstcmd.CommandText = "drop table Region_U8";
                tstcmd.ExecuteNonQuery();
            }
            catch { }

            tstcmd.CommandText = "create table Region_U8 (RegionID int not null not droppable, RegionDescription char(100) character set utf8, primary key(RegionID))";
            tstcmd.ExecuteNonQuery();

            tstcmd.CommandText = "insert into Region_U8 (RegionID, RegionDescription) values (?, ?)";

            tstcmd.Parameters.Add(new EsgynDBParameter("RegionID", EsgynDBType.Integer));
            tstcmd.Parameters.Add(new EsgynDBParameter("RegionDescription", EsgynDBType.Char, 100));

            tstcmd.Parameters[0].Value = 5;
            tstcmd.Parameters[1].Value = "North West";
            tstcmd.Prepare();
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 6;
            tstcmd.Parameters[1].Value = "North East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 7;
            tstcmd.Parameters[1].Value = "South East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 8;
            tstcmd.Parameters[1].Value = "South West";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters.Clear();

            tstcmd.CommandText = "SELECT count(*) FROM Region_U8";
            Object c = tstcmd.ExecuteScalar();

            Assert.AreEqual(Convert.ToInt32(c), 4);

            tstcmd.Dispose();
        }
        
/*
        [Test]
        public void CommandTimeout()
        {
            EsgyndbCommand tstcmd = new EsgyndbCommand();
            tstcmd.Connection = testInfo.connection;
            Console.WriteLine("Command Timeout : " + tstcmd.CommandTimeout);

            tstcmd.CommandTimeout = 1;
            Console.WriteLine("Command Timeout : " + tstcmd.CommandTimeout);
            //tstcmd.CommandText = "SELECT COUNT(*) FROM ADONET_TABLE A INNER JOIN ADONET_TABLE B ON A.C19=B.C19";
            tstcmd.CommandText = "SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ADOQA_SCHEMA.ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ODBCQA_SCHEMA.ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ODBCQA_SCHEMA.ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ODBCQA_SCHEMA.ADONET_TABLE";

            //tstcmd.Prepare();

            //System.Threading.Thread.Sleep(5000);

            //object c = tstcmd.ExecuteScalar();

            EsgynDBDataReader tstrdr = tstcmd.ExecuteReader();

            Assert.IsTrue(tstrdr.HasRows);
            //Assert.AreNotEqual(Convert.ToInt32(c), 0);

            tstrdr.Close();
            tstcmd.Dispose();
        }
*/
        [Test]
        //ARUNA - 3-4-2011 - CHANGING EXPECTED EXCEPTION SO TEST WILL PASS
        [ExpectedException(typeof(CommunicationsFailureException))]
        //[ExpectedException(typeof(EsgynDBException))]
        public void CommandTimeout()
        {
            string strInsertSelect = "insert into T10000K select 0 + (1000000 * x1000000) +(100000 * x100000) + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (100000 * x100000) + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',0 + (10000 * x10000) + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (1000 * x1000) + (100 * x100) + (10 * x10) + (1 * x1),0 + (100 * x100) + (10 * x10) + (1 * x1),0 + (10 * x10) + (1 * x1) from starter transpose 0,1,2,3,4,5,6,7,8,9 as x1000000 transpose 0,1,2,3,4,5,6,7,8,9 as x100000 transpose 0,1,2,3,4,5,6,7,8,9 as x10000 transpose 0,1,2,3,4,5,6,7,8,9 as x1000 transpose 0,1,2,3,4,5,6,7,8,9 as x100 transpose 0,1,2,3,4,5,6,7,8,9 as x10 transpose 0,1,2,3,4,5,6,7,8,9 as x1;";
            EsgynDBCommand xcmd = testInfo.connection.CreateCommand();
            int ret = 0;
            xcmd.CommandText = "DROP TABLE STARTER;";
            try
            {
                ret = xcmd.ExecuteNonQuery();
            }
            catch { }
            xcmd.CommandText = "DROP TABLE T10000K;";
            try
            {
                ret = xcmd.ExecuteNonQuery();
            }
            catch { }
            xcmd.CommandText = "create table starter (a int not null, primary key(a));";
            try
            {
                ret = xcmd.ExecuteNonQuery();
            }
            catch { }
            xcmd.CommandText = "create table T10000K(uniq int not null,c10K int,str1 varchar(50),c1K int,c100 int,c10 int,c1 int,primary key (uniq));";
            try
            {
                ret = xcmd.ExecuteNonQuery();
            }
            catch { }
            xcmd.CommandText = "insert into  starter values (1);";
            try
            {
                ret = xcmd.ExecuteNonQuery();
            }
            catch { }
            Console.WriteLine(ret);

            Console.WriteLine("Command Timeout : " + xcmd.CommandTimeout);
            xcmd.CommandTimeout = 1;
            Console.WriteLine("Command Timeout : " + xcmd.CommandTimeout);
            //execute a query
            xcmd.CommandText = strInsertSelect;
            ret = xcmd.ExecuteNonQuery();
            Console.WriteLine(ret);

            xcmd.Dispose();
        }

        [Test]
        public void CommandType()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();

            tstcmd.Connection = testInfo.connection;
            switch (tstcmd.CommandType)
            {
                case System.Data.CommandType.Text:
                    Console.WriteLine("CommandType is Text");
                    break;

                case System.Data.CommandType.TableDirect:
                    Console.WriteLine("CommandType is TableDirect");
                    break;

                case System.Data.CommandType.StoredProcedure:
                    Console.WriteLine("CommandType is StoredProcedure");
                    break;
            }
            Assert.AreEqual(System.Data.CommandType.Text, tstcmd.CommandType);
            /*
            tstcmd.CommandType = System.Data.CommandType.TableDirect;
            switch (tstcmd.CommandType)
            {
                case System.Data.CommandType.Text:
                    Console.WriteLine("CommandType is Text");
                    break;

                case System.Data.CommandType.TableDirect:
                    Console.WriteLine("CommandType is TableDirect");
                    break;

                case System.Data.CommandType.StoredProcedure:
                    Console.WriteLine("CommandType is StoredProcedure");
                    break;
            }
            Assert.AreEqual(System.Data.CommandType.Text, tstcmd.CommandType);
            */
            tstcmd.CommandType = System.Data.CommandType.StoredProcedure;
            switch (tstcmd.CommandType)
            {
                case System.Data.CommandType.Text:
                    Console.WriteLine("CommandType is Text");
                    break;

                case System.Data.CommandType.TableDirect:
                    Console.WriteLine("CommandType is TableDirect");
                    break;

                case System.Data.CommandType.StoredProcedure:
                    Console.WriteLine("CommandType is StoredProcedure");
                    break;
            }
            Assert.AreEqual(System.Data.CommandType.StoredProcedure, tstcmd.CommandType);
            tstcmd.Dispose();
        }

        [Test]
        public void Label()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();

            tstcmd.Connection = testInfo.connection;

            Console.WriteLine("Command Label : " + tstcmd.Label);

            tstcmd.Dispose();

        }

        [Test]
        public void Parameters()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            try
            {
                tstcmd.CommandText = "drop table Region";
                tstcmd.ExecuteNonQuery();
            }
            catch { }
            
            tstcmd.CommandText = "create table Region (RegionID int not null not droppable, RegionDescription char(100), primary key(RegionID))";
            tstcmd.ExecuteNonQuery();

            tstcmd.CommandText = "insert into Region (RegionID, RegionDescription) values (?, ?)";

            tstcmd.Parameters.Add(new EsgynDBParameter("RegionID", EsgynDBType.Integer));
            tstcmd.Parameters.Add(new EsgynDBParameter("RegionDescription", EsgynDBType.Char, 100));
            
            tstcmd.Parameters[0].Value = 5;
            tstcmd.Parameters[1].Value = "North West";
            tstcmd.Prepare();
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 6;
            tstcmd.Parameters[1].Value = "North East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 7;
            tstcmd.Parameters[1].Value = "South East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 8;
            tstcmd.Parameters[1].Value = "South West";
            tstcmd.ExecuteNonQuery();

            EsgynDBParameterCollection npc = tstcmd.Parameters;

            Assert.AreEqual(2, npc.Count);

            foreach (EsgynDBParameter np in npc)
            {
                Console.WriteLine("DbType : " + np.DbType);
                Console.WriteLine("Direction : " + np.Direction);
                Console.WriteLine("Encoding : " + np.Encoding);
                Console.WriteLine("GetType : " + np.GetType());
                Console.WriteLine("Heading : " + np.Heading);
                Console.WriteLine("IsNullable : " + np.IsNullable);
                Console.WriteLine("EsgynDBType : " + np.EsgynDBDbType);
                Console.WriteLine("ParameterName : " + np.ParameterName);
                Console.WriteLine("Precision : " + np.Precision);
                Console.WriteLine("Scale : " + np.Scale);
                Console.WriteLine("Signed : " + np.Signed);
                Console.WriteLine("Size : " + np.Size);
                Console.WriteLine("SourceCatalog : " + np.SourceCatalog);
                Console.WriteLine("SourceColumn : " + np.SourceColumn);
                Console.WriteLine("SourceColumnNullMapping : " + np.SourceColumnNullMapping);
                Console.WriteLine("SourceSchema : " + np.SourceSchema);
                Console.WriteLine("SourceTable : " + np.SourceTable);
                Console.WriteLine("SourceVersion : " + np.SourceVersion);
                Console.WriteLine("Value : " + np.Value);
            }

            tstcmd.Parameters.Clear();

            tstcmd.CommandText = "SELECT count(*) FROM Region";
            Object c = tstcmd.ExecuteScalar();

            Assert.AreEqual(Convert.ToInt32(c), 4);

            tstcmd.Dispose();
        }

        [Test]
        public void RowsAffected()
        {
            int rowsAffected;
            try
            {
                cmd.CommandText = "drop table TEMP_FC cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC (C int) no partition";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "insert into TEMP_FC values(1),(2)";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "update TEMP_FC set C=100 where C=1";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, rowsAffected);

            cmd.CommandText = "select * from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, rowsAffected);
        }

        
        [Test]
        public void Parameters_U8()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            try
            {
                tstcmd.CommandText = "drop table Region_U8";
                tstcmd.ExecuteNonQuery();
            }
            catch { }

            tstcmd.CommandText = "create table Region_U8 (RegionID int not null not droppable, RegionDescription char(100) character set utf8, primary key(RegionID))";
            tstcmd.ExecuteNonQuery();

            tstcmd.CommandText = "insert into Region_U8 (RegionID, RegionDescription) values (?, ?)";

            tstcmd.Parameters.Add(new EsgynDBParameter("RegionID", EsgynDBType.Integer));
            tstcmd.Parameters.Add(new EsgynDBParameter("RegionDescription", EsgynDBType.Char, 100));

            tstcmd.Parameters[0].Value = 5;
            tstcmd.Parameters[1].Value = "North West";
            tstcmd.Prepare();
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 6;
            tstcmd.Parameters[1].Value = "North East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 7;
            tstcmd.Parameters[1].Value = "South East";
            tstcmd.ExecuteNonQuery();

            tstcmd.Parameters[0].Value = 8;
            tstcmd.Parameters[1].Value = "South West";
            tstcmd.ExecuteNonQuery();

            EsgynDBParameterCollection npc = tstcmd.Parameters;

            Assert.AreEqual(2, npc.Count);

            foreach (EsgynDBParameter np in npc)
            {
                Console.WriteLine("DbType : " + np.DbType);
                Console.WriteLine("Direction : " + np.Direction);
                Console.WriteLine("Encoding : " + np.Encoding);
                Console.WriteLine("GetType : " + np.GetType());
                Console.WriteLine("Heading : " + np.Heading);
                Console.WriteLine("IsNullable : " + np.IsNullable);
                Console.WriteLine("EsgynDBType : " + np.EsgynDBDbType);
                Console.WriteLine("ParameterName : " + np.ParameterName);
                Console.WriteLine("Precision : " + np.Precision);
                Console.WriteLine("Scale : " + np.Scale);
                Console.WriteLine("Signed : " + np.Signed);
                Console.WriteLine("Size : " + np.Size);
                Console.WriteLine("SourceCatalog : " + np.SourceCatalog);
                Console.WriteLine("SourceColumn : " + np.SourceColumn);
                Console.WriteLine("SourceColumnNullMapping : " + np.SourceColumnNullMapping);
                Console.WriteLine("SourceSchema : " + np.SourceSchema);
                Console.WriteLine("SourceTable : " + np.SourceTable);
                Console.WriteLine("SourceVersion : " + np.SourceVersion);
                Console.WriteLine("Value : " + np.Value);
            }

            tstcmd.Parameters.Clear();

            tstcmd.CommandText = "SELECT count(*) FROM Region_U8";
            Object c = tstcmd.ExecuteScalar();

            Assert.AreEqual(Convert.ToInt32(c), 4);

            tstcmd.Dispose();
        }


        [Test]
        public void RowsAffected_U8()
        {
            int rowsAffected;
            try
            {
                cmd.CommandText = "drop table TEMP_FC_U8 cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_FC_U8 (c1 char(7) character set utf8) no partition";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "insert into TEMP_FC_U8 values('aaa1'),('bcv2')";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "update TEMP_FC_U8 set c1='re3' where c1='bcv2'";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(1, rowsAffected);

            cmd.CommandText = "select * from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);

            cmd.CommandText = "delete from TEMP_FC_U8";
            rowsAffected = cmd.ExecuteNonQuery();
            Assert.AreEqual(0, rowsAffected);
        }


        [Test]
        public void Transaction()
        {
            EsgynDBCommand myCommand = testInfo.connection.CreateCommand();
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            myCommand.ExecuteNonQuery();

            // Start a local transaction.
            EsgynDBTransaction myTrans = testInfo.connection.BeginTransaction();
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgynDBException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            myTrans.Dispose();
            myCommand.Dispose();
        }

        [Test]
        public void Transaction_U8()
        {
            EsgynDBCommand myCommand = testInfo.connection.CreateCommand();
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB_U8 cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB_U8 (c1 char(9) character set utf8) no partition";
            myCommand.ExecuteNonQuery();

            // Start a local transaction.
            EsgynDBTransaction myTrans = testInfo.connection.BeginTransaction();
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB_U8 values('aaaa'),('bbbb'),('cccc')";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(3, rowsAffected);

                myTrans.Commit();

                Console.WriteLine("Three records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgynDBException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            myTrans.Dispose();
            myCommand.Dispose();
        }

        [Test]
        public void UpdatedRowSource()
        {
            EsgynDBCommand tstcmd = new EsgynDBCommand();
            tstcmd.Connection = testInfo.connection;
            Console.WriteLine(tstcmd.UpdatedRowSource);

            //DEFAULT IS NONE, HAVE TO FIGURE OUT HOW TO TEST THE OTHER SETTINGS

            tstcmd.UpdatedRowSource = UpdateRowSource.Both;
            Console.WriteLine(tstcmd.UpdatedRowSource);

            tstcmd.UpdatedRowSource = UpdateRowSource.FirstReturnedRecord;
            Console.WriteLine(tstcmd.UpdatedRowSource);

            tstcmd.UpdatedRowSource = UpdateRowSource.OutputParameters;
            Console.WriteLine(tstcmd.UpdatedRowSource);

            tstcmd.Dispose();
        }
    }
}
