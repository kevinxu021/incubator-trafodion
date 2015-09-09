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
    class ParameterTest
    {
        public EsgyndbConnection conn;
        public EsgyndbCommand cmd;
        public EsgyndbParameter parm;
        /*
        private string server = "sqa0101.cup.hp.com";
        private string port = "18650";
        private string user = "qauser_user";
        private string password = "HPDb2009";
        private string catalog = "NEO";
        private string schema = "ADOQA_SCHEMA";
        */ 
        String connectionString;
      
        public void generate_data()
        {
            try
            {
                cmd.CommandText = "drop table test_parameter;";
                cmd.ExecuteNonQuery();
            }
            catch { }
            cmd.CommandText = "create table test_parameter (A INT, B INT) no partition";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into test_parameter values (20,2),(2,3);";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select count(*) from test_parameter";
            int returnvalue;
            returnvalue = cmd.ExecuteNonQuery();
            Assert.AreEqual(-1, returnvalue);
            Object obj = cmd.ExecuteScalar();
            Assert.AreEqual("2", obj.ToString());
            Console.WriteLine("create and insert data to test_parameter successfully");
        }

       

        [SetUp]
        public void SetUp()
        {
            /*
            connectionString = "server=" + server + ":" + port +
                                   ";user=" + user +
                                   ";password=" + password +
                                   ";catalog=" + catalog +
                                   ";schema=" + schema +
                                   ";"; 
            */

            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;

            conn = new EsgyndbConnection(connectionString);
            conn.Open();
            cmd = conn.CreateCommand();
            generate_data();
        }

        [TearDown]
        public void TearDown()
        {
           conn.Close();
        }

        [Test]
        public void ParameterNoParm()
        {
            parm = new EsgyndbParameter();
            cmd.Parameters.Add(parm);
            Console.WriteLine("parms count:"+cmd.Parameters.Count);
          
            Console.WriteLine("parm.value: {0}", parm.Value);
        }

        [Test]
        public void ParameterStringTypeParm()
        {
            parm = new EsgyndbParameter("testParm", EsgyndbDbType.Integer);
            parm.DbType = DbType.Int32;

            cmd.CommandText = "select count(*) from test_parameter where A != ?";
            parm.Value = 2;
            cmd.Parameters.Add(parm);
            Object obj = cmd.ExecuteScalar();
            Assert.AreEqual("1", obj.ToString());
            Console.WriteLine("parm.dbType: {0}, parm.name: {1}, parms.count:{2}", parm.DbType.ToString(), parm.ParameterName, cmd.Parameters.Count);
            Assert.AreEqual(parm.ParameterName,cmd.Parameters[0].ParameterName);
            Assert.AreEqual(parm.DbType.ToString(), cmd.Parameters[0].DbType.ToString());
        }

        [Test]
        public void ParameterStringObjectParm()
        {
            parm = new EsgyndbParameter("testParm",2);
            cmd.Parameters.Add(parm);
            Console.WriteLine("parm.value: {0}, parm.name: {1}, {2}", parm.Value, parm.ParameterName, cmd.Parameters.Count);
            Assert.AreEqual(cmd.Parameters[0].ParameterName, parm.ParameterName);
        }

        [Test]
        public void ParameterFourParm()
       // [Ignore]
        {
            //parm = new EsgyndbParameter("testParm", EsgyndbDbType.Double, 4, "test_parameter");
            //cmd.Parameters.Add(parm);
            //Console.WriteLine("parm.parmname:{0},parm.type:{1}", parm.ParameterName, parm.DbType);
            //Console.WriteLine("parm.size:{0}", parm.Size);
            //Console.WriteLine("parm.column:{0}", parm.SourceColumn);
            //Assert.AreEqual(parm.ParameterName, cmd.Parameters[0].ParameterName);
            //Assert.AreEqual(parm.DbType, cmd.Parameters[0].DbType);
            //Assert.AreEqual(parm.Size, cmd.Parameters[0].Size);
            //Assert.AreEqual(parm.SourceColumn, cmd.Parameters[0].SourceColumn);
        }

        [Test]
        public void ResetDbType()
        //Ignore, not support now
        {
            parm = new EsgyndbParameter("testParm", EsgyndbDbType.Integer);
            cmd.Parameters.Add(parm);
            try
            {
                parm.ResetDbType();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
    }
}
