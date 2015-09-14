using System;
using System.Collections.Generic;
using System.Text;
using System.Collections;

using EsgynDB.Data;
using NUnit.Framework;
using System.Data.Common;
using System.Data;


namespace TrafAdoTest
{
    [TestFixture]
    class FactoryTest
    {
        public EsgynDBFactory factory;
        public TestObject testinfo = AdoTestSetup.testInfo;
        //public EsgynDBConnection conn;

        [SetUp]
        public void Setup()
        {
            factory = new EsgynDBFactory();
            testinfo.connection.Open();
        }

        [TearDown]
        public void TearDown()
        {
            testinfo.connection.Close();
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
        public void CreateCommand()
        {
            int returnv;
            EsgynDBCommand cmd = (EsgynDBCommand) factory.CreateCommand();
            cmd.CommandText = testinfo.selectTable;
            cmd.Connection = testinfo.connection;

            returnv = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, returnv);
        }

        [Test]
        public void CreateCommandBuilder()
        {
            EsgynDBCommandBuilder cmdbuilder = (EsgynDBCommandBuilder) factory.CreateCommandBuilder();
            cmdbuilder.ConflictOption = ConflictOption.CompareRowVersion;
            Assert.IsNotNull(cmdbuilder);
        }

        [Test]
        public void CreateConnection()
        {
            EsgynDBConnection conn = (EsgynDBConnection)factory.CreateConnection();
            conn.ConnectionString = testinfo.connectionString;
            conn.Open();
            Assert.AreEqual(ConnectionState.Open, conn.State);
            conn.Close();
        }

        [Test]
        public void CreateConnectionStringBuilder()
        {
            EsgynDBConnectionStringBuilder constrbuilder = (EsgynDBConnectionStringBuilder)factory.CreateConnectionStringBuilder();
            constrbuilder.ConnectionString = testinfo.connectionString;
            Assert.AreEqual("True", constrbuilder.ContainsKey("Server").ToString());
        }

        [Test]
        public void CreateDataAdapter()
        {
            EsgynDBDataAdapter dataadapter = (EsgynDBDataAdapter)factory.CreateDataAdapter();
            EsgynDBCommand cmd = testinfo.connection.CreateCommand();
            cmd.CommandText = testinfo.selectTable;
            Console.WriteLine("cmd's commandText is:" + cmd.CommandText);

            DataTable dt= new DataTable();
            dataadapter.SelectCommand = cmd;
            dataadapter.Fill(dt);
            displayDatatable(dt);
            //Assert.AreEqual(10, dt.Rows.Count);
            Assert.AreNotEqual(0, dt.Rows.Count);
        }

        [Test]
        [ExpectedException(typeof(NotSupportedException))]
        public void CreateDataSourceEnumerator()
        {
            DbDataSourceEnumerator dataSourses = factory.CreateDataSourceEnumerator();
        }

        [Test]
        public void CanCreateDataSourceEnumerator()
        {
            Assert.IsFalse(factory.CanCreateDataSourceEnumerator);
        }

        [Test]
        public void CreateParameter()
        {
            EsgynDBCommand cmd = testinfo.connection.CreateCommand();
            cmd.CommandText = "select count(*) from ADONET_TABLE where C01=?"; 
            EsgynDBParameter parameter = (EsgynDBParameter)factory.CreateParameter();
            parameter.Direction = ParameterDirection.Input;
            parameter.DbType = DbType.String;
            parameter.ParameterName = "p1";
            parameter.Value = "!#$%&a()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQR";
            cmd.Parameters.Add(parameter);

            Object obj =cmd.ExecuteScalar();
            Console.WriteLine(obj.ToString());
            Assert.AreEqual("0", obj.ToString());
        }

        [Test]
        public void CreateFactory()
        {
            EsgynDBFactory fac;
            fac = new EsgynDBFactory();
            if  (factory.Equals(fac))
                Console.WriteLine("fac is equal with factory");
            else
                Console.WriteLine("fac is not equal with factory");

        }

    }
}
