using System;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;
using Trafodion.Data;

namespace TrafAdoTest
{
    [TestFixture]
    public class CommandBuilderTest
    {
        TestObject testInfo = AdoTestSetup.testInfo;
        private TrafDbCommand cmd;
        //private TrafDbDataReader reader;

        [SetUp]
        public void SetUp()
        {
            testInfo.connection.Open();
            cmd = testInfo.connection.CreateCommand();
            cmd.CommandTimeout = 0;
            //cmd.CommandText = testInfo.selectTable;
        }

        [TearDown]
        public void TearDown()
        {
            cmd.Dispose();
            testInfo.connection.Close();
        }

        [Test]
        public void CommandBuilder1()
        {
            cmd.CommandText = "drop table t1";
            try { cmd.ExecuteNonQuery(); }
            catch { } //ignore error
            cmd.CommandText = "create table t1 (c1 int not null not droppable primary key, c2 varchar(100))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(1,'bleh')";
            cmd.ExecuteNonQuery();

            TrafDbCommand selectCmd = testInfo.connection.CreateCommand();
            selectCmd.CommandText = "select * from t1";

            TrafDbDataAdapter adp = new TrafDbDataAdapter();
            TrafDbCommandBuilder cb = new TrafDbCommandBuilder(adp);

            adp.SelectCommand = selectCmd;
            adp.UpdateCommand = (TrafDbCommand)cb.GetUpdateCommand();
            adp.DeleteCommand = (TrafDbCommand)cb.GetDeleteCommand();
            adp.InsertCommand = (TrafDbCommand)cb.GetInsertCommand();

            Console.WriteLine("Insert Command: " + adp.InsertCommand.CommandText);
            Console.WriteLine("Update Command: " + adp.UpdateCommand.CommandText);
            Console.WriteLine("Delete Command: " + adp.DeleteCommand.CommandText);

        }
        [Test]
        public void CommandBuilder2()
        {
            cmd.CommandText = "drop table t2_u8";
            try { cmd.ExecuteNonQuery(); }
            catch { } //ignore error
            cmd.CommandText = "create table t2_u8 (c1 varchar(10) character set utf8 not null not droppable primary key, c2 varchar(100) character set utf8)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t2_u8 values('ˮ��1','����1')";
            cmd.ExecuteNonQuery();

            TrafDbCommand selectCmd = testInfo.connection.CreateCommand();
            selectCmd.CommandText = "select * from t1";

            TrafDbDataAdapter adp = new TrafDbDataAdapter();
            TrafDbCommandBuilder cb = new TrafDbCommandBuilder(adp);

            adp.SelectCommand = selectCmd;
            adp.UpdateCommand = (TrafDbCommand)cb.GetUpdateCommand();
            adp.DeleteCommand = (TrafDbCommand)cb.GetDeleteCommand();
            adp.InsertCommand = (TrafDbCommand)cb.GetInsertCommand();

            Console.WriteLine("Insert Command: " + adp.InsertCommand.CommandText);
            Console.WriteLine("Update Command: " + adp.UpdateCommand.CommandText);
            Console.WriteLine("Delete Command: " + adp.DeleteCommand.CommandText);

        }
     }
}
