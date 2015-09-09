using System;
using System.Data;
using System.Text;
using System.Configuration;

using NUnit.Framework;
using Esgyndb.Data;

namespace TrafAdoTest
{
    [TestFixture]
    public class ReaderTest
    {
        private EsgyndbCommand cmd;
        private EsgyndbDataReader reader;
        private EsgyndbConnection conn;

        [SetUp]
        public void SetUp()
        {
            int rowsAffected = 0;
            conn = new EsgyndbConnection(ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString);
            conn.Open();
            cmd = conn.CreateCommand();
            cmd.CommandTimeout = 0;
            try
            {
                cmd.CommandText = "drop table TEMP_TS cascade";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table TEMP_TS (C timestamp) no partition";
            rowsAffected = cmd.ExecuteNonQuery();

            cmd.CommandText = "insert into TEMP_TS values(TIMESTAMP '2008-04-03:19:12:10.123'), (TIMESTAMP '2008-04-03:19:12:10.123000')";
            rowsAffected = cmd.ExecuteNonQuery();

            cmd.CommandText = "SELECT * from TEMP_TS";
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
            conn.Close();
        }

        [Test]
        public void GetValue()
        {
            while (reader.Read())
            {
                Console.WriteLine(reader.GetValue(0).GetType().Name + " " + "2008-04-03 19:12:10.123000" + " " + ((DateTime)(reader.GetValue(0))).ToString("yyyy-MM-dd HH:mm:ss.ffffff"));
                if (reader.GetValue(0).GetType().Name.Equals("DateTime"))
                {
                    Assert.AreEqual(DateTime.Parse("2008-04-03 19:12:10.123000"), DateTime.Parse(((DateTime)(reader.GetValue(0))).ToString("yyyy-MM-dd HH:mm:ss.ffffff")));
                }
                else
                {
                    Assert.AreEqual("2008-04-03 19:12:10.123000", ((DateTime)(reader.GetValue(0))).ToString());
                }
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

        [Test]
        public void GetValueBigNum()
        {

            cmd.CommandText = "SELECT C26 from ADONET_TABLE";
            reader = cmd.ExecuteReader();
            DataTable dt = reader.GetSchemaTable();
            displayDatatable(dt);
            Console.WriteLine("****************************************");
            while (reader.Read())
            {
                //Object obj = reader.GetValue(0);
                //object obj = reader.GetDecimal(0);
                //object obj = reader.GetBigNumeric(0);
                object obj = reader.GetString(0);
                Console.WriteLine(reader.GetValue(0).GetType().Name + " " + obj.ToString());
            }

        }
    }
}
