using System;
using System.Collections.Generic;
using System.Text;

using Esgyndb.Data;
using NUnit.Framework;
using System.Data;
using System.Configuration;

namespace TrafAdoTest
{
    [TestFixture]
    class DataAdapterTest
    {
        EsgyndbConnection conn;
        String connectionString;
        EsgyndbDataAdapter dataadapter;
        EsgyndbCommand cmd;

        [SetUp]
        public void SetUp()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            conn = new EsgyndbConnection(connectionString);
            conn.Open();
            cmd = conn.CreateCommand();
            generate_data();
        }

        public void generate_data()
        {
            int returnvalue;
            try
            {
                cmd.CommandText = "drop table test_DataAdapter;";
                cmd.ExecuteNonQuery();
            }
            catch { } 
            cmd.CommandText = "create table test_DataAdapter (A INT NOT NULL NOT DROPPABLE, B INT, PRIMARY KEY(A))";
            returnvalue = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, returnvalue);
            cmd.CommandText = "insert into test_DataAdapter values (20,2),(2,3);";
            returnvalue = cmd.ExecuteNonQuery();
            Assert.AreEqual(2, returnvalue);
            cmd.CommandText = "select count(*) from test_DataAdapter";
            Object obj= cmd.ExecuteScalar();
            Assert.AreEqual("2", obj.ToString()); 
            Console.WriteLine("create and insert data to test_DataAdapter successfully");
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

        [TearDown]
        public void TearDown()
        {
            try
            {
                //cmd.CommandText = "drop table test_DataAdapter;";
                //cmd.ExecuteNonQuery();
                //Console.WriteLine("drop table test_DataAdapter successfully");
            }
            catch { }
            cmd.Dispose();
            conn.Close();
        }
        [Test]
        public void DataAdapter_Fill_Update()
        {
            cmd.CommandText = "drop table t1";
            try { cmd.ExecuteNonQuery(); }
            catch { } //ignore error
            cmd.CommandText = "create table t1 (c1 int not null not droppable primary key, c2 varchar(100))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(1,'bleh')";
            cmd.ExecuteNonQuery();

            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from t1";

            EsgyndbDataAdapter adp = new EsgyndbDataAdapter();
            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(adp);

            adp.SelectCommand = selectCmd;
            adp.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            adp.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            adp.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();

            Console.WriteLine("Update Command is : " + adp.UpdateCommand.CommandText);
            Console.WriteLine("Delete Command is : " + adp.DeleteCommand.CommandText);
            Console.WriteLine("Insert Command is : " + adp.InsertCommand.CommandText);

            DataSet ds = new DataSet();

            //select the data
            adp.Fill(ds);
            //delete a row
            ds.Tables[0].Rows[0].Delete();
            //insert a row
            ds.Tables[0].Rows.Add(new object[] { 2, "test test" });
            //update row
            ds.Tables[0].Rows[1]["c2"] = "hi there";

            //send values back to Esgyndb
            adp.Update(ds);

            //check the final results -- we should have a single row (2,'hi there')
            cmd.CommandText = "select * from t1";
            EsgyndbDataReader dr = cmd.ExecuteReader();
            int numrows = 0;
            while (dr.Read())
            {
                Console.WriteLine("{0} {1}", dr[0], dr[1]);
                numrows++;
            }
            Assert.AreEqual(1, numrows);
        }

        [Test]
        public void UpdateBatchSize()
        {
            int bs = 1;
            DataTable dt;
            dt = new DataTable();

            EsgyndbCommand tmpcmd = new EsgyndbCommand();
            tmpcmd.Connection = conn;

            tmpcmd.CommandText = "DROP TABLE ProdCat";
            try
            {
                tmpcmd.ExecuteNonQuery();
            }
            catch{}
            tmpcmd.CommandText = "CREATE TABLE ProdCat(ProdCatID int not null not droppable, Name char(50), primary key(ProdcatID))";
            tmpcmd.ExecuteNonQuery();
            tmpcmd.CommandText = "INSERT INTO ProdCat VALUES (1,'abc'),(2,'xyz')";
            tmpcmd.ExecuteNonQuery();


            EsgyndbCommand selcmd = new EsgyndbCommand("select * from ProdCat", conn);
           
            dataadapter = new EsgyndbDataAdapter(selcmd);
            Console.WriteLine("value of UpdatebatchSize is : " + dataadapter.UpdateBatchSize);
            dataadapter.UpdateBatchSize = 10;
            Console.WriteLine("value of UpdatebatchSize is : " + dataadapter.UpdateBatchSize);

            EsgyndbCommandBuilder cb = new EsgyndbCommandBuilder(dataadapter);

            dataadapter.SelectCommand = selcmd;
            dataadapter.UpdateCommand = (EsgyndbCommand)cb.GetUpdateCommand();
            dataadapter.DeleteCommand = (EsgyndbCommand)cb.GetDeleteCommand();
            dataadapter.InsertCommand = (EsgyndbCommand)cb.GetInsertCommand();
            
            dataadapter.Fill(dt);
            /*
            dataadapter.UpdateCommand = new EsgyndbCommand("UPDATE ProdCat SET Name=? WHERE ProdCatID=?;", conn);
            dataadapter.UpdateCommand.Parameters.Add(new EsgyndbParameter("Name",EsgyndbDbType.NVarchar, 50));
            dataadapter.UpdateCommand.Parameters.Add(new EsgyndbParameter("ProdCatID",EsgyndbDbType.Integer));
             * */

            Console.WriteLine("value of dataadapter.UpdateCommand.UpdatedRowSource is : " + dataadapter.UpdateCommand.UpdatedRowSource);
            dataadapter.UpdateCommand.UpdatedRowSource = UpdateRowSource.None;
            Console.WriteLine("value of dataadapter.UpdateCommand.UpdatedRowSource is : " + dataadapter.UpdateCommand.UpdatedRowSource);
            /*
            dataadapter.InsertCommand = new EsgyndbCommand("INSERT INTO ProdCat VALUES (?,?);",conn);
            dataadapter.UpdateCommand.Parameters.Add(new EsgyndbParameter("ProdCatID", EsgyndbDbType.Integer, 4, "ProdcatID"));
            dataadapter.InsertCommand.Parameters.Add(new EsgyndbParameter("Name", EsgyndbDbType.NVarchar, 50, "Name"));
             * */
            Console.WriteLine("value of dataadapter.InsertCommand.UpdatedRowSource is : " + dataadapter.InsertCommand.UpdatedRowSource);
            dataadapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;
            Console.WriteLine("value of dataadapter.InsertCommand.UpdatedRowSource is : " + dataadapter.InsertCommand.UpdatedRowSource);
            /*
            dataadapter.DeleteCommand = new EsgyndbCommand("DELETE FROM ProdCat WHERE ProdCatID=?;", conn);
            dataadapter.DeleteCommand.Parameters.Add(new EsgyndbParameter("ProdCatID",EsgyndbDbType.Integer,4,"ProdCatID"));
             * */
            Console.WriteLine("value of dataadapter.DeleteCommand.UpdatedRowSource is : " + dataadapter.DeleteCommand.UpdatedRowSource);
            dataadapter.DeleteCommand.UpdatedRowSource = UpdateRowSource.None;
            Console.WriteLine("value of dataadapter.DeleteCommand.UpdatedRowSource is : " + dataadapter.DeleteCommand.UpdatedRowSource);

            dt.Rows[0].Delete();
            //insert a row
            dt.Rows.Add(new object[] { 3, "test test" });
            //update row
            dt.Rows[1]["Name"] = "hi there";

            // Set the batch size.
            dataadapter.UpdateBatchSize = bs;

            // Execute the update.
            dataadapter.Update(dt);
        }

        [Test]
        public void GetBatchedParameter()
        {
        
        }

        [Test]
        public void GetFillParameter()
       //GetFillParameters returns the DataParameter[]
        {
            cmd.CommandText = "delete from test_DataAdapter";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into test_DataAdapter values (20,2),(2,3);";
            cmd.ExecuteNonQuery();

            dataadapter = new EsgyndbDataAdapter(cmd);
            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from test_DataAdapter where A > ?";
            selectCmd.Parameters.Add(new EsgyndbParameter("A", EsgyndbDbType.Integer));
            dataadapter.SelectCommand = selectCmd;

            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            dataadapter.GetFillParameters()[0].Value = 2;
            dataadapter.Fill(dt1);

            dataadapter.GetFillParameters()[0].Value = 0;
            dataadapter.Fill(dt2);

            displayDatatable(dt1);
            displayDatatable(dt2);

            Assert.AreEqual(1, dt1.Rows.Count);
            Assert.AreEqual(2, dt2.Rows.Count);

            //parameters = dataadapter.GetFillParameters();
            //Console.WriteLine("adapter's GetFillParameter's value:" + parameters[0].Value);//+" SourceColumn:"+parameters[0].SourceColumn+" parameterName:"+parameters[0].ParameterName+" size:"+parameters[0].Size);
            //Console.WriteLine("adapter's cmd param value:" + dataadapter.DeleteCommand.Parameters[0].Value);
        }

        [Test]
        public void DataAdapterWithNoParameter()
        {
            dataadapter = new EsgyndbDataAdapter();
            EsgyndbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter.SelectCommand = selectCmd;

            EsgyndbCommand updateCmd = conn.CreateCommand();
            updateCmd.CommandText = "update test_DataAdapter set A=? where A=20";
            updateCmd.Parameters.Add(new EsgyndbParameter("A", EsgyndbDbType.Integer));
            dataadapter.UpdateCommand = updateCmd;
            DataTable dt_update;
            dt_update = new DataTable();
            dataadapter.Fill(dt_update);
            Assert.AreEqual(2, dt_update.Rows.Count);

            dt_update.Rows[0]["A"] = 2000;
            dataadapter.Update(dt_update);
            
            displayDatatable(dt_update);
            dataadapter.Dispose();
            dt_update.Clear();

            //for update command
            EsgyndbCommand sCmd = conn.CreateCommand();
            sCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter = new EsgyndbDataAdapter();
            dataadapter.SelectCommand = sCmd;
            dataadapter.Fill(dt_update);
            EsgyndbCommand uCmd = conn.CreateCommand();
            uCmd.CommandText = "update test_DataAdapter set A=? where A=2000";
            uCmd.Parameters.Add(new EsgyndbParameter("A", EsgyndbDbType.Integer));
            dataadapter.UpdateCommand = uCmd;
            dt_update.Rows[0]["A"] = 200;
            dataadapter.Update(dt_update);
            Console.WriteLine("after update, the table is:");
            cmd.CommandText = "select count(*) from test_DataAdapter where A=200;";
            object obj = cmd.ExecuteScalar();
            Assert.AreEqual("1", obj.ToString());
            Console.WriteLine("the test_DataAdapter table have rows:" + obj.ToString());
            dataadapter.Dispose();
            dt_update.Clear();

            //for delete command
            dataadapter = new EsgyndbDataAdapter();
            EsgyndbCommand selCmd = conn.CreateCommand();
            selCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter.SelectCommand = selCmd;
            dataadapter.Fill(dt_update);
            displayDatatable(dt_update);
            EsgyndbCommand delCmd = conn.CreateCommand();
            delCmd.CommandText = "delete from test_DataAdapter where A=?;";
            dataadapter.DeleteCommand = delCmd;
            delCmd.Parameters.Add(new EsgyndbParameter("A", EsgyndbDbType.Integer));
            dt_update.Rows[0].Delete();
            dataadapter.Update(dt_update);
            cmd.CommandText = "select count(*) from test_DataAdapter";
            Console.WriteLine("after delete,the test_DataAdapter table have rows:" + cmd.ExecuteScalar().ToString());
            Console.Write("\n");
            Console.WriteLine("after delete,the dt_update table have rows:" + dt_update.Rows.Count);
            Console.Write("\n");
            Assert.AreEqual(1, dt_update.Rows.Count);

            dataadapter.Dispose();
        }

        [Test]
        public void DataAdapterWithParameter()
        {
            cmd.CommandText = "select * from test_DataAdapter;";
            Console.WriteLine("cmd's commandtext is:" + cmd.CommandText);
            dataadapter = new EsgyndbDataAdapter(cmd);
            DataTable dt;
            dt = new DataTable();
            dataadapter.Fill(dt);
            displayDatatable(dt);
            Assert.AreEqual(2, dt.Rows.Count);
        }

    }
}
