using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Resources;
using Trafodion.Data;
using System.Data.SqlClient;
using System.Collections;
using Microsoft.Win32;
using System.IO;
using Trafodion.Data.VisualStudio;
using System.Threading;
using System.Diagnostics;
using Trafodion.Data.ETL;
using System.Data.Odbc;
using System.Collections.Generic;
using System.Configuration;
using System.Text;

namespace ConsoleApp
{
    class Program
    {
        //sqws02.americas.hpqcorp.net:30000
        //public static string ConnectionString = "server=arc0101.cup.hp.com;user=SUPERUSER;password=HPNe@v1ew;catalog=NEO;schema=DOERMANN";
        //public static string ConnectionString = "server=mer0101.cup.hp.com;user=super.super;password=mcK1nley;catalog=NEO;schema=ODBC_SCHEMA";
        //public static string ConnectionString = "server=sqws18.caclab.cac.cpqcorp.net:61300;user=sql_user;password=redhat06;catalog=NEO;schema=daniel";
        //public static string ConnectionString = "server=ruby-mxoas3.houston.hp.com:18650;user=sqdev3;password=redhat06;catalog=neo;schema=ADOQA_SCHEMA";
        //public static string ConnectionString = "server=sqws23.caclab.cac.cpqcorp.net:20000;user=sqluser_admin;password=sq2010;catalog=NEO;schema=ODBC_SCHEMA";
        //public static string ConnectionString = "server=sqa0101.cup.hp.com;user=qauser_user;password=HPDb2009;catalog=NEO;schema=ODBCQA_SCHEMA;";
        public static string ConnectionString = "server=sqws118.houston.hp.com:42972;user=zz;password=zz;schema=ado";
        public static void TestBatch()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c1 varchar(20), c2 nchar(20)) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", TrafDbDbType.Varchar));
                        cmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Varchar));

                        cmd.Prepare();

                        for (int i = 0; i < 10; i++)
                        {
                            cmd.Parameters[0].Value = "test col1";
                            cmd.Parameters[1].Value = "test col2";
                            cmd.AddBatch();
                        }

                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " " + dr.GetDataTypeName(i));
                                }

                                Console.Write("\r\n");
                            }

                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void DropTable(TrafDbCommand cmd, string name)
        {
            try
            {
                cmd.CommandText = "drop table " + name + " cascade";
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        public static void TestTransaction()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c0 numeric(19,8), c1 numeric(34,13), c2 numeric(87,45), c3 numeric(104,3), c4 numeric(128,64)) no partition";
                        cmd.ExecuteNonQuery();

                        TrafDbTransaction trans = conn.BeginTransaction(IsolationLevel.ReadCommitted);

                        cmd.CommandText = "insert into t0 values(12345.6789, -123.67891234123, 123456789.023, 12.34, 123456789.123456)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(45345.1, 38423, -8457234.43, 99.99, 1.54367), (0,0,0,0,0)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?,?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", 12345.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c1", -123.67891234123));
                        cmd.Parameters.Add(new TrafDbParameter("c2", 123456789.023));
                        cmd.Parameters.Add(new TrafDbParameter("c3", 12.34));
                        cmd.Parameters.Add(new TrafDbParameter("c4", 123456789.123456));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = 45345.1;
                        cmd.Parameters[1].Value = 38423;
                        cmd.Parameters[2].Value = -8457234.43;
                        cmd.Parameters[3].Value = 99.99;
                        cmd.Parameters[4].Value = 1.54367;
                        cmd.ExecuteNonQuery();

                        trans.Commit();

                        cmd.Parameters.Clear();

                        Console.WriteLine("REGULAR");
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " ");
                                }

                                Console.Write("\r\n");
                            }
                        }

                        Console.WriteLine("CommandBehavior.SingleRow");
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleRow))
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " ");
                                }

                                Console.Write("\r\n");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestBulkInsert()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        DropTable(cmd, "dude");

                        cmd.CommandText = "create table dude (id int not null, name varchar(100), primary key(id))";
                        cmd.ExecuteNonQuery();

                        TrafDbCommand sel = conn.CreateCommand();
                        sel.CommandText = "select * from dude";

                        TrafDbCommand ins = conn.CreateCommand();
                        ins.CommandText = "insert into dude (id, name) VALUES (?,?)";
                        ins.Parameters.Add(new TrafDbParameter("id", TrafDbDbType.Integer));
                        ins.Parameters.Add(new TrafDbParameter("name", TrafDbDbType.Varchar));

                        TrafDbDataAdapter adp = new TrafDbDataAdapter();
                        adp.SelectCommand = sel;
                        adp.InsertCommand = ins;

                        DataTable dt = new DataTable();
                        adp.Fill(dt);

                        dt.Rows.Add(new object[] { 1, "hi" });
                        dt.Rows.Add(new object[] { 2, "bleh" });
                        
                        adp.Update(dt);
                    }
                }
            }
            catch (Exception e)
            {
                Console.Write(e.ToString());
            }
        }

        public static void TestSqlServer()
        {
            SqlConnection cn = new SqlConnection();
            SqlCommand cmd = new SqlCommand();
            DataTable schemaTable;
            SqlDataReader myReader;

            //Open a connection to the SQL Server Northwind database.
            cn.ConnectionString = "Data Source=MDOERMANN\\SSDEFAULT;Integrated Security=SSPI;Initial Catalog=master";
            cn.Open();

            //Retrieve records from the Employees table into a DataReader.
            cmd.Connection = cn;
            cmd.CommandText = "SELECT * FROM dbo.spt_fallback_db";
            myReader = cmd.ExecuteReader(CommandBehavior.KeyInfo);

            //Retrieve column schema into a DataTable.
            schemaTable = myReader.GetSchemaTable();

            //For each field in the table...
            foreach (DataRow myField in schemaTable.Rows)
            {
                //For each property of the field...
                foreach (DataColumn myProperty in schemaTable.Columns)
                {
                    //Display the field name and value.
                    Console.WriteLine(myProperty.ColumnName + " = " + myField[myProperty].ToString());
                }
                Console.WriteLine();

                //Pause.
                Console.ReadLine();
            }

            //Always close the DataReader and connection.
            myReader.Close();
            cn.Close();
        }

        public static void TestKeyInfo()
        {
            try
            {
                TestBulkInsert();


                string [] tables = {"REGION", "TEAM", "PLAYER"};
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        /*DropTable(cmd, "player");
                        DropTable(cmd, "team");
                        DropTable(cmd, "region");

                        cmd.CommandText = "create table region (id int not null, name varchar(100), primary key(id))";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "create table team (id int not null, region_id int not null, name varchar(100), primary key(id), foreign key(region_id) references region(id))";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "create table player (id int not null, team_id int not null, name varchar(100), primary key(id), foreign key(team_id) references team(id))";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into region values(?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("id", TrafDbDbType.Integer));
                        cmd.Parameters.Add(new TrafDbParameter("name", TrafDbDbType.Varchar));

                        cmd.Parameters[0].Value = 1;
                        cmd.Parameters[1].Value = "North America";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into team values(?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("id", TrafDbDbType.Integer));
                        cmd.Parameters.Add(new TrafDbParameter("region", TrafDbDbType.Integer));
                        cmd.Parameters.Add(new TrafDbParameter("name", TrafDbDbType.Varchar));

                        cmd.Parameters[0].Value = 1;
                        cmd.Parameters[1].Value = 1;
                        cmd.Parameters[2].Value = "United States";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters[0].Value = 2;
                        cmd.Parameters[1].Value = 1;
                        cmd.Parameters[2].Value = "Mexico";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters[0].Value = 3;
                        cmd.Parameters[1].Value = 1;
                        cmd.Parameters[2].Value = "Canada";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into player values(?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("id", TrafDbDbType.Integer));
                        cmd.Parameters.Add(new TrafDbParameter("team", TrafDbDbType.Integer));
                        cmd.Parameters.Add(new TrafDbParameter("name", TrafDbDbType.Varchar));

                        cmd.Parameters[0].Value = 1;
                        cmd.Parameters[1].Value = 1;
                        cmd.Parameters[2].Value = "US Player 1";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters[0].Value = 2;
                        cmd.Parameters[1].Value = 1;
                        cmd.Parameters[2].Value = "US Player 2";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters[0].Value = 3;
                        cmd.Parameters[1].Value = 2;
                        cmd.Parameters[2].Value = "Mexico Player 1";
                        cmd.ExecuteNonQuery();
                        cmd.Parameters[0].Value = 4;
                        cmd.Parameters[1].Value = 3;
                        cmd.Parameters[2].Value = "Canada Player 1";
                        cmd.ExecuteNonQuery();*/

                        cmd.CommandText = "select r.name, t.name, p.name " +
                            "from player as p inner join team as t on p.team_id=t.id " +
                            "inner join region as r on t.region_id = r.id " +
                            "order by r.name, t.name, p.name";

                        TrafDbDataReader dr = cmd.ExecuteReader();
                        string [] vals = new string[3];
                        while (dr.Read())
                        {
                            dr.GetValues(vals);
                            Console.WriteLine("'{0}' '{1}' '{2}'", vals);
                        }

                        foreach(string t in tables) 
                        {
                            string [] res = new string[]{ "NEO","DOERMANN",t};

                            PrintDataTable(conn.GetSchema("PrimaryKeys", res));
                           // PrintDataTable(conn.GetSchema("ForeignKeys", res));
                            //PrintDataTable(conn.GetSchema("ForeignKeyColumns", res));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.Write(e.ToString());
            }

        }

        public static void TestBigNum()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c0 numeric(19,8), c1 numeric(34,13), c2 numeric(87,45), c3 numeric(104,3), c4 numeric(128,64)) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(12345.6789, -123.6789, 123456789.023, 12.34, 123456789.123456)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(45345.1, 38423, -8457234.43, 99.99, 1.54367), (0,0,0,0,0)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?,?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", 12345.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c1", -123.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c2", 123456789.023));
                        cmd.Parameters.Add(new TrafDbParameter("c3", 12.34));
                        cmd.Parameters.Add(new TrafDbParameter("c4", 123456789.123456));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = 45345.1;
                        cmd.Parameters[1].Value = 38423;
                        cmd.Parameters[2].Value = -8457234.43;
                        cmd.Parameters[3].Value = 99.99;
                        cmd.Parameters[4].Value = 1.54367;
                        cmd.ExecuteNonQuery();

                        Console.WriteLine(TrafDbDbType.Integer);


                        cmd.CommandText = "select * from t0";
                        cmd.Parameters.Clear();
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            PrintDataTable(dr.GetSchemaTable());
                            if (dr.HasRows)
                            {
                                IEnumerator e = dr.GetEnumerator();
                                while(e.MoveNext()) 
                                {
                                    for (int i = 0; i < dr.FieldCount; i++)
                                    {
                                        Console.Write(dr.GetValue(i) + " ");
                                    }

                                    Console.Write("\r\n");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestDynamicNumeric()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString; 
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        //float, decimal, numeric
                        cmd.CommandText = "create table t0 (c0 float, c1 decimal(10,5), c2 decimal(18,3), c3 numeric(4,2), c4 numeric(18,6)) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(12345.6789, -12345.6789, 123456789.023, 12.34, 123456789.123456)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(45345.1, 38423, -8457234.43, 99.99, 1.54367)";
                        cmd.ExecuteNonQuery();

                        /*12345.6789 -2345.6789 123456789.023 12.34 123456789.123456
                        45345.1 38423 8457234.43 99.99 1.54367
                        12345.6789 -2345.6789 123456789.023 12.34 123456789.123456
                        45345.1 38423 8457234.43 99.99 1.54367*/

                        cmd.CommandText = "insert into t0 values(?,?,?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", 12345.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c1", -12345.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c2", 123456789.023));
                        cmd.Parameters.Add(new TrafDbParameter("c3", 12.34));
                        cmd.Parameters.Add(new TrafDbParameter("c4", 123456789.123456));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = 45345.1;
                        cmd.Parameters[1].Value = 38423;
                        cmd.Parameters[2].Value = -8457234.43;
                        cmd.Parameters[3].Value = 99.99;
                        cmd.Parameters[4].Value = 1.54367;
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " ");
                                }

                                Console.Write("\r\n");
                            }

                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestStaticNumericUnsigned()
        {
            try {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c0 smallint unsigned, c1 int unsigned) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(2345, 2345)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(" + UInt16.MaxValue + "," + UInt32.MaxValue + ")";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(" + UInt16.MinValue + "," + UInt32.MinValue + ")";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", 2345));
                        cmd.Parameters.Add(new TrafDbParameter("c1", 2345));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = UInt16.MaxValue;
                        cmd.Parameters[1].Value = UInt32.MaxValue;
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(new TrafDbParameter("c0", ushort.MinValue));
                        cmd.Parameters.Add(new TrafDbParameter("c1", uint.MinValue));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " ");
                                }

                                Console.Write("\r\n");
                            }

                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestStaticNumeric()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString; 
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c0 smallint, c1 int, c2 largeint, c3 real, c4 double precision) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(2345, 2345, 2345, 2345.6789, 2345.6789)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(" + Int16.MaxValue + "," + Int32.MaxValue + "," + Int64.MaxValue + "," +
                            float.MaxValue + "," + (double.MaxValue/10) + ")";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(null,null,null,null,null)";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(" + Int16.MinValue + "," + Int32.MinValue + "," + Int64.MinValue + "," +
                            float.MinValue + "," + (double.MinValue/10) + ")";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?,?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", 2345));
                        cmd.Parameters.Add(new TrafDbParameter("c1", 2345));
                        cmd.Parameters.Add(new TrafDbParameter("c2", 2345));
                        cmd.Parameters.Add(new TrafDbParameter("c3", 2345.6789));
                        cmd.Parameters.Add(new TrafDbParameter("c4", 2345.6789));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = Int16.MaxValue;
                        cmd.Parameters[1].Value = Int32.MaxValue;
                        cmd.Parameters[2].Value = Int64.MaxValue;
                        cmd.Parameters[3].Value = float.MaxValue;
                        cmd.Parameters[4].Value = (double.MaxValue/10);
                        cmd.ExecuteNonQuery();

                        cmd.Parameters[0].Value = null;
                        cmd.Parameters[1].Value = DBNull.Value;
                        cmd.Parameters[2].Value = DBNull.Value;
                        cmd.Parameters[3].Value = null;
                        cmd.Parameters[4].Value = null;
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.Parameters.Add(new TrafDbParameter("c0", short.MinValue));
                        cmd.Parameters.Add(new TrafDbParameter("c1", int.MinValue));
                        cmd.Parameters.Add(new TrafDbParameter("c2", long.MinValue));
                        cmd.Parameters.Add(new TrafDbParameter("c3", float.MinValue));
                        cmd.Parameters.Add(new TrafDbParameter("c4", (double.MinValue/10)));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " ");
                                }

                                Console.Write("\r\n");
                            }

                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestDateTime()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString; 
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        DateTime dt = DateTime.Now;

                        Console.WriteLine(dt.ToString());

                        cmd.CommandText = "create table t0 (c0 date, c1 time(0), c2 time(3), c3 time(6), c4 timestamp(0), c5 timestamp(3), c6 timestamp(6)) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values({d'" +
                            dt.ToString("yyyy-MM-dd") + "'}, {t'" +
                            dt.ToString("HH:mm:ss") + "'}, {t'" +
                            dt.ToString("HH:mm:ss.fff") + "'}, {t'" +
                            dt.ToString("HH:mm:ss.ffffff") + "'}, {ts'" +
                            dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'}, {ts '" +
                            dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'}, {ts '" +
                            dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "'})";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?,?,?,?,?,?)";
                        for(int i=0;i<7;i++ ) 
                        {
                            cmd.Parameters.Add(new TrafDbParameter("c" + i, dt));
                        }
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    object o = dr.GetValue(i);
                                    if (o is DateTime)
                                    {
                                        Console.Write(((DateTime)o).ToString("yyyy-MM-dd HH:mm:ss.ffffff") + " ");
                                    }
                                    else
                                    {
                                        Console.Write(o + " ");
                                    }
                                }

                                Console.Write("\r\n");
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void TestString()
        {
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString; 
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        try
                        {
                            cmd.CommandText = "drop table t0";
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e.Message);
                        }

                        cmd.CommandText = "create table t0 (c0 char(20000), c1 varchar(20), c2 nchar(20), c3 nchar varying(20)) no partition";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values('test string', 'test string', 'test string', 'test string')";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t0 values(?,?,?,?)";
                        cmd.Parameters.Add(new TrafDbParameter("c0", "test string"));
                        cmd.Parameters.Add(new TrafDbParameter("c1", "test string"));
                        cmd.Parameters.Add(new TrafDbParameter("c2", "test string"));
                        cmd.Parameters.Add(new TrafDbParameter("c3", "test string"));
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t0";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                for (int i = 0; i < dr.FieldCount; i++)
                                {
                                    Console.Write(dr.GetValue(i) + " " + dr.GetDataTypeName(i));
                                }

                                Console.Write("\r\n");
                            }
                            
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }


        private static readonly Assembly asm = Assembly.GetAssembly(typeof(TrafDbFactory)); //the assembly to register

        public static void DataAdapterWithNoParameter()
        {
            TrafDbConnection conn = new TrafDbConnection();
            conn.ConnectionString = ConnectionString;
            conn.Open();
            TrafDbCommand cmd = conn.CreateCommand();

            TrafDbDataAdapter dataadapter = new TrafDbDataAdapter();
            TrafDbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter.SelectCommand = selectCmd;

            TrafDbCommand updateCmd = conn.CreateCommand();
            updateCmd.CommandText = "update test_DataAdapter set A=? where A=20";
            updateCmd.Parameters.Add(new TrafDbParameter("A", TrafDbDbType.Integer));
            dataadapter.UpdateCommand = updateCmd;
            DataTable dt_update;
            dt_update = new DataTable();
            dataadapter.Fill(dt_update);
            //Assert.AreEqual(2, dt_update.Rows.Count);

            dt_update.Rows[0]["A"] = 2000;
            dataadapter.Update(dt_update);

            //displayDatatable(dt_update);
            dataadapter.Dispose();
            dt_update.Clear();

            //for update command
            TrafDbCommand sCmd = conn.CreateCommand();
            sCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter = new TrafDbDataAdapter();
            dataadapter.SelectCommand = sCmd;
            dataadapter.Fill(dt_update);
            TrafDbCommand uCmd = conn.CreateCommand();
            uCmd.CommandText = "update test_DataAdapter set A=? where A=2000";
            uCmd.Parameters.Add(new TrafDbParameter("A", TrafDbDbType.Integer));
            dataadapter.UpdateCommand = uCmd;
            dt_update.Rows[0]["A"] = 200;
            dataadapter.Update(dt_update);
            Console.WriteLine("after update, the table is:");
            cmd.CommandText = "select count(*) from test_DataAdapter where A=200;";
            object obj = cmd.ExecuteScalar();
            //Assert.AreEqual("1", obj.ToString());
            Console.WriteLine("the test_DataAdapter table have rows:" + obj.ToString());
            dataadapter.Dispose();
            dt_update.Clear();

            //for delete command
            dataadapter = new TrafDbDataAdapter();
            TrafDbCommand selCmd = conn.CreateCommand();
            selCmd.CommandText = "select * from test_DataAdapter;";
            dataadapter.SelectCommand = selCmd;
            dataadapter.Fill(dt_update);
            //displayDatatable(dt_update);
            TrafDbCommand delCmd = conn.CreateCommand();
            delCmd.CommandText = "delete from test_DataAdapter where A=?;";
            dataadapter.DeleteCommand = delCmd;
            delCmd.Parameters.Add(new TrafDbParameter("A", TrafDbDbType.Integer));
            dt_update.Rows[0].Delete();
            dataadapter.Update(dt_update);
            cmd.CommandText = "select count(*) from test_DataAdapter";
            Console.WriteLine("after delete,the test_DataAdapter table have rows:" + cmd.ExecuteScalar().ToString());
            Console.Write("\n");
            Console.WriteLine("after delete,the dt_update table have rows:" + dt_update.Rows.Count);
            Console.Write("\n");
            //Assert.AreEqual(1, dt_update.Rows.Count);

            dataadapter.Dispose();
        }

        private static readonly string[] MachingConfigs = 
        {
            @"Framework\v2.0.50727\CONFIG\machine.config",
            @"Framework64\v2.0.50727\CONFIG\machine.config",
            @"Framework\v4.0.30319\Config\machine.config",
            @"Framework64\v4.0.30319\Config\machine.config"
        };

        private static string[] GetMachineConfigs()
        {
            // find the current machine.config and move up 3 directories to get the root of all 
            // the installed frameworks
            DirectoryInfo currentConfig = new DirectoryInfo(Path.GetDirectoryName(ConfigurationManager.OpenMachineConfiguration().FilePath));
            string root = currentConfig.Parent.Parent.Parent.FullName;

            List<string> paths = new List<string>(4);
            string s;

            // check all the possible release configurations
            foreach (string config in MachingConfigs)
            {
                s = Path.Combine(root, config);
                Console.WriteLine(s);
                if (File.Exists(s))
                {
                    paths.Add(s);
                }
            }

            return paths.ToArray();
        }


        static void Main(string[] args)
        {
            
            TrafDbConnection conn = new TrafDbConnection();

            conn.ConnectionString = Program.ConnectionString;

            conn.Open();
            //Console.WriteLine(conn.Database);

            String value = "!\"#$%&a()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQR";
            String strCrtSchm = "create schema tsch";
            String strDropTbl = "drop table tsch.atest";
            String strCrtTbl = "create table tsch.atest(c1 char(500) CHARACTER SET UCS2 COLLATE default not null)";
            String strInst = "Insert into tsch.atest values('" + value + "')";
            String strSelct = "select * from tsch.atest";
            String strSelCnt1 = "select count(*) from tsch.atest where c1=?";
            String strSelCnt2 = "select count(*) from tsch.atest where c1='" + value + "'";

            TrafDbCommand cmd = conn.CreateCommand();
            
            //cmd.CommandText = strCrtSchm;
            //cmd.ExecuteNonQuery();

            //cmd.CommandText = strDropTbl;
            //cmd.ExecuteNonQuery();

            cmd.CommandText = strCrtTbl;
            Console.WriteLine("Executing: " + strCrtTbl + "...");
            cmd.ExecuteNonQuery();

            cmd.CommandText = strInst;
            Console.WriteLine("Executing: " + strInst + "...");
            cmd.ExecuteNonQuery();

            Console.WriteLine("Value " + value + "inserted.");            
            
            cmd.CommandText = strSelct;
            Console.WriteLine("Executing: " + strSelct + "...");
            TrafDbDataReader reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine("GetString(c1) : " + reader.GetString(0));
                /*
                char[] buffer = new char[2048];
                long retLen = reader.GetChars(0, 0, buffer, 0, buffer.Length);

                Console.WriteLine("GetChars(c1) : " + retLen + " chars returned.");
                Console.WriteLine("\tValue = " + new String(buffer));
                 * */
            }

            cmd.CommandText = strSelCnt2;
            Console.WriteLine("Executing: " + strSelCnt2 + "...");
            reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                Console.WriteLine("{0} count of rows found: ", reader.GetInt64(0));
            }
            reader.Close();

            cmd.CommandText = strSelCnt1;
            Console.WriteLine("Executing: " + strSelCnt1 + "...");
            cmd.Parameters.Clear();
            TrafDbParameter pam = new TrafDbParameter("C1Value", TrafDbDbType.Char);
            //pam.DbType = DbType.String;
            //pam.Value = value;
            Encoding utf8 = Encoding.GetEncoding("UTF-8");
            Encoding utf16 = Encoding.GetEncoding("UTF-16");
            byte[] utf8Bytes = utf8.GetBytes(value);
            byte[] utf16Bytes = Encoding.Convert(utf8, utf16, utf8Bytes);
            //pam.Value = utf16.GetString(utf16Bytes);
            pam.Value = utf8.GetString(utf8Bytes);

            byte[] vBytes = Encoding.Default.GetBytes(value);
            //byte[] vPamBytes = Encoding.Default.GetBytes((String)pam.Value);

            Console.WriteLine("Default Bytes:");
            for (int i = 0; i < vBytes.Length; i++)
            {
                if (i % 10 != 0)
                    Console.Write(vBytes[i] + " ");
                else
                    Console.WriteLine("");
            }
            Console.WriteLine("\n");

            Console.WriteLine("UTF-8 Bytes:");
            for (int i = 0; i < utf8Bytes.Length; i++)
            {
                if (i % 10 != 0)
                    Console.Write(utf8Bytes[i] + " ");
                else
                    Console.WriteLine("");
            }
            Console.WriteLine("\n");

            Console.WriteLine("UTF-16 Bytes:");
            for (int i = 0; i < utf16Bytes.Length; i++)
            {
                if (i % 10 != 0)
                    Console.Write(utf16Bytes[i] + " ");
                else
                    Console.WriteLine("");
            }
            Console.WriteLine("\n");

            cmd.Parameters.Add(pam);

            Object obj = cmd.ExecuteScalar();
            Console.WriteLine("{0} count of rows found: ", obj.ToString());


            /*
            System.Data.DataTable dt = conn.GetSchema();
            //dt.TableName
            foreach (System.Data.DataRow row in dt.Rows)
            {
                foreach (System.Data.DataColumn col in dt.Columns)
                {
                    Console.WriteLine("{0} = {1}", col.ColumnName, row[col]);
                }
                Console.WriteLine("============================");
            }
            */
            
                
            /*try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = ConnectionString;
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "drop table t1";
                        try { cmd.ExecuteNonQuery(); }
                        catch { }

                        cmd.CommandText = "create table t1 (c1 timestamp(3)) no partition;";
                        cmd.ExecuteNonQuery();

                        cmd.CommandText = "insert into t1 values(?)";
                        cmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Timestamp));
                        cmd.Parameters[0].Value = "2008-04-03 19:12:10.123";
                        cmd.ExecuteNonQuery();

                        cmd.Parameters.Clear();
                        cmd.CommandText = "select * from t1";
                        using (TrafDbDataReader dr = cmd.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                cmd.Cancel();
                                Console.WriteLine(dr.GetDateTime(0).ToString("yyyy-MM-dd hh:mm:ss.fff"));
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
             * */

            /*
            Console.WriteLine("\r\n");
            TestDynamicNumeric();
            Console.WriteLine("\r\n");
            TestStaticNumericUnsigned();
            Console.WriteLine("\r\n");
            TestStaticNumeric();
            Console.WriteLine("\r\n");
            TestDateTime();
            Console.WriteLine("\r\n");
            TestString();
            Console.WriteLine("\r\n");
            
            TestBatch();
            
           TestAdapterBuilder();
                        * */

            //TestPerformance();

            //TestMetaData();

            Console.Read();
        }

        private static void TestPerformance()
        {
            string connStr = "server=sqws18.caclab.cac.cpqcorp.net:61300;user=sql_user;password=redhat06;catalog=neo;schema=daniel";
            //string connStr = "server=ruby-mxoas3.houston.hp.com:18650;user=sqdev3;password=redhat06;catalog=neo;schema=ADOQA_SCHEMA";
            string query = "SELECT * FROM UTF8TEST";
            string[] connOptions = 
            {
                //";compression=false;prefetch=false;fetchrowcount=100", 
                //";compression=true;prefetch=false;fetchrowcount=1000",
                //";compression=true;prefetch=false;fetchrowcount=5000",
                //";compression=true;prefetch=false;fetchrowcount=5000",
                //";compression=true;prefetch=true;fetchrowcount=5000",
                //";compression=true;prefetch=false;fetchrowcount=5000;datasource=doermann",
                ";compression=true;prefetch=false;fetchrowcount=5000;LoginTimeout=-1;IdleTimeout=-1;RetryCount=10;RetryTime=5000",
                
                //";compression=true;prefetch=true;fetchrowcount=10000",
            };
            int[] parallelStreams = { 8 };
            string file;

            foreach(string opt in connOptions) 
            {
                foreach (int streams in parallelStreams)
                {
                    file = string.Format(@"c:\logs\data{0}.txt", DateTime.Now.Ticks);
                    if (streams == 1)
                    {
                        RegularExtract(ConnectionString + opt, query, file);
                    }
                    else
                    {
                        ParallelExtract(ConnectionString + opt, query, streams, file);
                    }
                    Thread.Sleep(5000);
                }
            }
        }

        private static void RegularExtract(string connStr, string query, string file)
        {
            Console.WriteLine(string.Format("Conn: {0}\n Query: {1}", connStr, query));

            FileStream fs = File.Open(file, FileMode.Create);
            TextWriter tw = new StreamWriter(fs);

            string format = "{0}|\"{1}\"|\"{2}\"";

            Stopwatch sw = new Stopwatch();
            sw.Start();
            
            using (TrafDbConnection conn = new TrafDbConnection())
            {
                conn.ConnectionString = connStr;
                conn.Open();

                using (TrafDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = query;

                    using (TrafDbDataReader dr = cmd.ExecuteReader())
                    {
                        int cacheSize = 1024 * 1024;

                        // TODO: calculate this in a better way
                        StringBuilder sb = new StringBuilder(cacheSize);
                        object[] v = new object[dr.FieldCount];

                        while (dr.Read())
                        {
                            for (int i = 0; i < dr.FieldCount; i++)
                            {
                                v[i] = dr.GetValue(i);
                            }
                            sb.AppendFormat(format, v);

                            // TODO: this is a bad check as we always expand the buffer once
                            if (sb.Length > cacheSize)
                            {
                                tw.Write(sb.ToString());
                                tw.Flush();

                                sb.Length = 0; // reset
                            }
                        }

                        if (sb.Length > 0)
                        {
                            tw.Write(sb.ToString());
                            tw.Flush();
                        }
                    }

                    fs.Flush();
                    fs.Close();

                    sw.Stop();
                    Console.WriteLine("\tTime: " + sw.ElapsedMilliseconds);
                }
            }
        }

        private static void ParallelExtract(string connStr, string query, int streams, string file)
        {
            try
            {
                Console.WriteLine(string.Format("Conn: {0}\nQuery: {1}\nStreams: {2}", connStr, query, streams));

                FileStream fs = File.Open(file, FileMode.Create);

                Stopwatch sw = new Stopwatch();
                sw.Start();

                TrafDbParallelExtract pe = new TrafDbParallelExtract()
                {
                    ParallelStreams = streams,
                    ConnectionString = connStr + ";application name=TRANSPORTER",
                    CommandText = query,
                    FieldDelimiter = "|",
                    QuoteIdentifier = "\"",
                    QuoteEscapeSequence = "\"\"\"",
                    RowDelimiter = "\r\n",
                };

                TrafDbDataReader[] readers=pe.Execute();

                pe.WriteToStream(fs);
                pe.Close();

                fs.Flush();
                fs.Close();

                sw.Stop();
                Console.WriteLine("\tTime: " + sw.ElapsedMilliseconds);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        private static void TestConnectionPooling()
        {
            for (int i = 0; i < 5; i++)
            {
                Stopwatch sw = new Stopwatch();

                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = ConnectionString +";Pooling=true;MaxPoolSize=10";
                    sw.Start();
                    conn.Open();

                    sw.Stop();

                    Console.WriteLine(sw.ElapsedMilliseconds);

                    TrafDbCommand cmd = conn.CreateCommand();
               
                        cmd.CommandText = "drop table t1";
                        try { cmd.ExecuteNonQuery(); }
                        catch { }

                        cmd.CommandText = "create table t1 (c1 timestamp(3)) no partition;";
                        cmd.ExecuteNonQuery();
                

                    conn.Close();
                }
            }
        }

        private static void TestExtract()
        {
            string connStr = "server=tra0101.cup.hp.com;user=SUPERUSER;password=HPNe@v1ew;catalog=neo;schema=doermann";

            //SetupExtract(connStr);

            //Console.WriteLine("ODBC Bridge Extract");
            //ODBCExtract();


            string[] query = { "SELECT * FROM NEO.DOERMANN.ETABLE ORDER BY c1" /*"SELECT [first 100000] * FROM archivetbl" "SELECT [first 100000] * FROM NEO.AR_LOG_SCHEMA.NPTEST", /*"SELECT * FROM NEO.DOERMANN.ETABLE2", "SELECT * FROM NEO.DOERMANN.ETABLE3"*/ };
            string[] conns = { ";compression=false;prefetch=false;defaultfetchsize=2000", 
                                 ";compression=true;prefetch=false;defaultfetchsize=5000",
                                 ";compression=true;prefetch=true;defaultfetchsize=5000"
                             };
            int fetchsize = 2000;
            
            for (int k = 0; k < query.Length; k++)
            {
                for (int i = 2; i < conns.Length; i++)
                {
                    RegularExtract(connStr + conns[i], @"c:\logs\nocompress.txt", query[k], fetchsize);
                    Thread.Sleep(2000);
                    /*for (int j = 8; j <= 8; j*=2)
                    {
                        ParallelExtract(j, connStr + conns[i], @"c:\logs\nvtnet-test" + k+i+j + ".txt", fetchsize, query[k]);
                        Thread.Sleep(2000);
                    }*/
                }
            }
        }

        private static void SetupExtract(string connStr)
        {
            using (TrafDbConnection conn = new TrafDbConnection())
            {
                conn.ConnectionString = connStr;
                conn.Open();

                using (TrafDbCommand cmd = conn.CreateCommand())
                {
                    /*cmd.CommandText = "drop table etable";
                    try { cmd.ExecuteNonQuery(); }
                    catch { } // ignore

                    cmd.CommandText = "create table etable (c1 int not null primary key, c2 varchar(1000), c3 varchar(1000))";
                    cmd.ExecuteNonQuery();
                    
                    cmd.CommandText = "insert into etable values(?,?,?)";
                    cmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));
                    cmd.Parameters.Add(new TrafDbParameter("c2", TrafDbDbType.Char));
                    cmd.Parameters.Add(new TrafDbParameter("c3", TrafDbDbType.Char));
                    cmd.Prepare();

                    cmd.Parameters[1].Value = "value1234567890";
                    cmd.Parameters[2].Value = "abcdefghijklmno";

                    cmd.Parameters[0].Value = 1;
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "insert into etable select etable.c1 + ?, etable.c2, etable.c3 from etable";
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));
                    cmd.Prepare();

                    for (int i = 1; i < 300000; i *= 2)
                    {
                        cmd.Parameters[0].Value = i;
                        cmd.ExecuteNonQuery();
                    }

                    cmd.Parameters.Clear();
                    cmd.CommandText = "select count(*) from etable";
                    Console.WriteLine("rows: " + cmd.ExecuteScalar());

                    cmd.CommandText = "drop table etable2";
                    try { cmd.ExecuteNonQuery(); }
                    catch { } // ignore

                    cmd.CommandText = "create table etable2 (c1 int not null primary key, c2 char(1000), c3 char(1000))";
                    cmd.ExecuteNonQuery();
                    
                    cmd.CommandText = "insert into etable2 select * from etable";
                    cmd.ExecuteNonQuery();*/

                    cmd.CommandText = "drop table etable3";
                    try { cmd.ExecuteNonQuery(); }
                    catch { } // ignore

                    cmd.CommandText = "create table etable3 (c1 int not null primary key, c2 varchar(30000))";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "insert into etable3 values(?,?)";
                    cmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));
                    cmd.Parameters.Add(new TrafDbParameter("c2", TrafDbDbType.Varchar));
                   
                    cmd.Prepare();

                    FileStream fs = File.Open(@"C:\ISO\Visual Studio 2008 (HP Edition).rar", FileMode.Open);
                    byte [] b = new byte[30000];
                    for (int i = 0; i < 15000; i++)
                    {
                        cmd.Parameters[0].Value = i;
                        fs.Read(b, 0, b.Length);
                        cmd.Parameters[1].Value = Encoding.ASCII.GetString(b);
                        cmd.ExecuteNonQuery();
                        if (i % 100 == 0)
                        {
                            Console.WriteLine(i);
                        }
                    }
                   
                    cmd.Parameters.Clear();
                    cmd.CommandText = "select count(*) from etable3";
                    Console.WriteLine("rows: " + cmd.ExecuteScalar());
                }
            }
        }

        private static void ODBCExtract()
        {
            OdbcConnectionStringBuilder builder = new OdbcConnectionStringBuilder();
            builder.Add("Driver", @"HP ODBC 2.0");
            builder.Add("SERVER", "TCP:tra0101.cup.hp.com/18650");
            builder.Add("UID", "SUPERUSER");
            builder.Add("PWD", "HPNe@v1ew");

           
            using (OdbcConnection conn = new OdbcConnection())
            {
                conn.ConnectionString = builder.ConnectionString;
                conn.Open();

                using (OdbcCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT * from neo.doermann.etable";

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    OdbcDataReader dr = cmd.ExecuteReader();
             
                    object[] v = new object[dr.FieldCount];

                    while (dr.Read())
                    {
                        dr.GetValues(v);
                    }

                    sw.Stop();
                    Console.WriteLine("Time: " + sw.ElapsedMilliseconds);
                }
            }
        }

        private static void RegularExtract(string connStr, string file, string query, int fetchsize)
        {
            Console.WriteLine(string.Format("Conn String: {0}\n Streams: {1}\nFetchSize: {2}\nQuery: {3}", connStr, 1, fetchsize, query));
                
            string format = "{0}|\"{1}\"";//|\"{2}\"";
            using (TrafDbConnection conn = new TrafDbConnection())
            {
                conn.ConnectionString = connStr;
                conn.Open();

                FileStream fs = File.Open(file, FileMode.Create);
                TextWriter tw = new StreamWriter(fs);

                using (TrafDbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = query;

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    TrafDbDataReader dr = cmd.ExecuteReader();
                    object [] v = new object[dr.FieldCount];

                    //dr.FetchSize = fetchsize;

                    while (dr.Read())
                    {
                        dr.GetValues(v);
                        tw.WriteLine(string.Format(format, v));
                    }

                    sw.Stop();
                    Console.WriteLine("\tTime: " + sw.ElapsedMilliseconds);
                }

                tw.Flush();
                fs.Flush();
                fs.Close();
            }
        }

        private static void ParallelExtract(int streams, string connStr, string file, int fetchSize, string query)
        {
            try
            {
                Console.WriteLine(string.Format("Streams: {0}\nFetchSize: {1}\nQuery: {2}", streams, fetchSize, query));
                
                FileStream fs = File.Open(file, FileMode.Create);               

                Stopwatch sw = new Stopwatch();
                sw.Start();

                TrafDbParallelExtract pe = new TrafDbParallelExtract()
                {
                    ParallelStreams = streams,
                    ConnectionString = connStr + ";application name=ADO.NET Parallel Extract",
                    CommandText = query,
                    FieldDelimiter = "|",
                    QuoteIdentifier = "\"",
                    QuoteEscapeSequence = "\"\"\"",
                    RowDelimiter = "\r\n",
                };

                pe.Execute();

                pe.WriteToStream(fs);
                pe.Close();

                fs.Flush();
                fs.Close();

                sw.Stop();
                Console.WriteLine("\tTime: " + sw.ElapsedMilliseconds);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        private static void Finished(IAsyncResult ar)
        {
            Console.WriteLine("FINISHED");
        }

        public static void TestCancel()
        {
            TrafDbConnection conn = new TrafDbConnection();
            conn.ConnectionString = Program.ConnectionString; 
            conn.Open();

            TrafDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "insert into neo.doermann.testcancel4 (c1) select x.c1 from neo.doermann.testcancel1 x, neo.doermann.testcancel2 y, neo.doermann.testcancel1 z where x.c1 + y.c1 + z.c1 < 70";

            Thread t = new Thread(new ParameterizedThreadStart(CancelCommand));
            t.Start(cmd);

            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        //wait 5 seconds, then cancel the command
        public static void CancelCommand(object var)
        {
            TrafDbCommand cmd = (TrafDbCommand)var;
            Thread.Sleep(10000);

            Console.WriteLine("issuing cancel");
            cmd.Cancel();
        }

        public static void TestRowsAffected()
        {
            TrafDbConnection conn = new TrafDbConnection();
            conn.ConnectionString = Program.ConnectionString; 
            conn.Open();

            TrafDbCommand cmd = conn.CreateCommand();
            try
            {
                cmd.CommandText = "drop table t0";
                Console.WriteLine(cmd.ExecuteNonQuery());
            }
            catch { }

            cmd.CommandText = "create table t0 (c1 int) no partition";
            Console.WriteLine(cmd.ExecuteNonQuery());

            cmd.CommandText = "insert into t0 values(0)";
            Console.WriteLine(cmd.ExecuteNonQuery());

            cmd.CommandText = "insert into t0 values(1)";
            Console.WriteLine(cmd.ExecuteNonQuery());

            using (TrafDbDataReader dr = cmd.ExecuteReader())
            {
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        dr.GetValue(0);
                    }
                }

                Console.WriteLine(dr.RecordsAffected);
            }

            cmd.CommandText = "select * from t0";
            using (TrafDbDataReader dr = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
            {
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        dr.GetValue(0);
                    }
                }

                Console.WriteLine(dr.RecordsAffected);
            }

            using (TrafDbDataReader dr = cmd.ExecuteReader(CommandBehavior.SingleRow))
            {
                if (dr.HasRows)
                {
                    while (dr.Read())
                    {
                        dr.GetValue(0);
                    }
                }

                Console.WriteLine(dr.RecordsAffected);
            }

            cmd.CommandText = "delete from t0";
            Console.WriteLine(cmd.ExecuteNonQuery());
        }

        private static string GetString(string str)
        {
            string [] s = str.Split(new char[]{','}, 3);
            AssemblyName name = new AssemblyName(s[2].Trim());
            Assembly asm = Assembly.Load(name);
            ResourceManager rm = new ResourceManager(s[1].Trim(), asm);

            return rm.GetString(s[0].Trim());
        }
        

        public static void TestSPJ1()
        {
            
            try
            {
                using (TrafDbConnection conn = new TrafDbConnection())
                {
                    conn.ConnectionString = Program.ConnectionString; 
                    conn.Open();

                    using (TrafDbCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandType = CommandType.StoredProcedure; //this actually isnt required
                        cmd.CommandText = "call NEO.ODBCSPJ.RS4(?,?)";
                        
                        //try some of the various ways to create parameters
                        cmd.Parameters.Add(new TrafDbParameter("p1", "NEO.DOERMANN.T1"));

                        TrafDbParameter parameter = cmd.CreateParameter();
                        parameter.Direction = ParameterDirection.Output;
                        parameter.DbType = DbType.Int32;
                        parameter.ParameterName = "p2";
                        parameter.Value = null;
                        cmd.Parameters.Add(parameter);

                        string pass = "password";
                        byte[] rawBytes = System.Text.ASCIIEncoding.ASCII.GetBytes(pass);
                        string base64 = conn.EncryptData(rawBytes);

                        parameter.Value = base64;

                        TrafDbDataReader dr = cmd.ExecuteReader();

                        Console.WriteLine(parameter.Value); //reference the original object
                        Console.WriteLine(cmd.Parameters[1].Value); //by index

                        //access the resultset
                        do
                        {
                            while (dr.Read())
                            {
                                Console.WriteLine(dr.GetValue(0));
                            }
                        } while (dr.NextResult());
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static void PrintDataTable(DataTable dt)
        {
            foreach (DataColumn c in dt.Columns)
            {
                Console.Write(c.ColumnName + " (" + c.DataType + ")  ");
            }
            Console.WriteLine("");
            foreach (DataRow r in dt.Rows)
            {
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                   Console.Write(r[i] + " ");
                }
                Console.WriteLine("");
            }
        }

        public static void TestMetaData()
        {
            TrafDbConnection conn = new TrafDbConnection();
            conn.ConnectionString = Program.ConnectionString; 
            conn.Open();

            //PrintDataTable(conn.GetSchema("restrictions"));

            /*PrintDataTable(conn.GetSchema());
            PrintDataTable(conn.GetSchema("reservEDWORDS"));
            
            PrintDataTable(conn.GetSchema("DATAtypes"));*/
            //PrintDataTable(conn.GetSchema("DataSourceInformation"));

            PrintDataTable(conn.GetSchema("catalogs", new string[] {"NEO"}));
            PrintDataTable(conn.GetSchema("catalogs", new string[] { "NEO" }));
            PrintDataTable(conn.GetSchema("catalogs", new string[] { "N%" }));
            PrintDataTable(conn.GetSchema("schemas"));
            //PrintDataTable(conn.GetSchema("tables", new string[] { "NEO", "ODBC_SCHEMA" }));
            //PrintDataTable(conn.GetSchema("columns", new string[] { "NEO", "ODBC_SCHEMA", "ADONET_TABLE" }));

            /*PrintDataTable(conn.GetSchema("schemas", new string[] { "NEO" }));
            PrintDataTable(conn.GetSchema("schemas", new string[] { "NEO", "DOERMANN" }));
            PrintDataTable(conn.GetSchema("Views", new string[] {"NEO"}));
            PrintDataTable(conn.GetSchema("tables", new string[] { "NEO", "ODBCQA_SCHEMA"}));
            PrintDataTable(conn.GetSchema("columns", new string[] { "NEO", "DOERMANN", "T1" }));*/
        }

        public static void TestAdapterManual()
        {
            TrafDbConnection conn = new TrafDbConnection(Program.ConnectionString);
            conn.Open();
            TrafDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "drop table t1";
            try { cmd.ExecuteNonQuery(); } catch { } //ignore error
            cmd.CommandText = "create table t1 (c1 int not null not droppable primary key, c2 varchar(100)) no partition";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(1,'bleh')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(99,'bleh')";
            cmd.ExecuteNonQuery();

            Console.WriteLine("Setup Complete");

            TrafDbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from t1";

            TrafDbCommand insertCmd = conn.CreateCommand();
            insertCmd.CommandText = "insert into t1 values(?,?)";
            insertCmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));
            insertCmd.Parameters.Add(new TrafDbParameter("c2", TrafDbDbType.Varchar));

            TrafDbCommand deleteCmd = conn.CreateCommand();
            deleteCmd.CommandText = "delete from t1 where c1=?";
            deleteCmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));

            TrafDbCommand updateCmd = conn.CreateCommand();
            updateCmd.CommandText = "update t1 set c2=? where c1=?";
            updateCmd.Parameters.Add(new TrafDbParameter("c2", TrafDbDbType.Varchar));
            updateCmd.Parameters.Add(new TrafDbParameter("c1", TrafDbDbType.Integer));
            
 
            TrafDbDataAdapter adp = new TrafDbDataAdapter();

            adp.SelectCommand = selectCmd;
            adp.UpdateCommand = updateCmd;
            adp.DeleteCommand = deleteCmd;
            adp.InsertCommand = insertCmd;

            DataTable dt = new DataTable();

            //select
            adp.Fill(dt);
            //delete
            dt.Rows[0].Delete();
            //insert
            dt.Rows.Add(new object[] { 2, "test2" });
            dt.Rows.Add(new object[] { 3, "test3" });
            dt.Rows.Add(new object[] { 4, "test4" });
            //update
            dt.Rows[1]["c2"] = "hi there";
            dt.Rows[3]["c2"] = "dude";

            //send values back to TrafDb
            adp.Update(dt);

            //check the final results -- we should have a single row (2,'hi there')
            cmd.CommandText = "select * from t1";
            TrafDbDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Console.WriteLine("{0} {1}", dr[0], dr[1]);
            }
        }

        public static void TestAdapterBuilder()
        {
            TrafDbConnection conn = new TrafDbConnection(Program.ConnectionString);
            conn.Open();
            TrafDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "drop table t1";
            try { cmd.ExecuteNonQuery(); }
            catch { } //ignore error
            cmd.CommandText = "create table t1 (c1 int not null not droppable primary key, c2 varchar(100)) no partition";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(1,'bleh')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(99,'bleh')";
            cmd.ExecuteNonQuery();

            Console.WriteLine("Setup Complete");

            TrafDbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from t1";

            TrafDbDataAdapter adp = new TrafDbDataAdapter();
            TrafDbCommandBuilder cb = new TrafDbCommandBuilder(adp);

            adp.SelectCommand = selectCmd;

            adp.UpdateCommand = (TrafDbCommand)cb.GetUpdateCommand();
            Console.WriteLine(adp.UpdateCommand.ToString());
            adp.DeleteCommand = (TrafDbCommand)cb.GetDeleteCommand();
            adp.InsertCommand = (TrafDbCommand)cb.GetInsertCommand();

            adp.UpdateBatchSize = 10;

            DataTable dt = new DataTable();

            //select
            adp.Fill(dt);
            //delete
            dt.Rows[0].Delete();
            //insert
            dt.Rows.Add(new object[] { 2, "test2" });
            dt.Rows.Add(new object[] { 3, "test3" });
            dt.Rows.Add(new object[] { 4, "test4" });
            //update
            dt.Rows[1]["c2"] = "value1";
            dt.Rows[3]["c2"] = "value2";

            //send values back to TrafDb
            //adp.Update(dt);

            //check the final results
            cmd.CommandText = "select * from t1";
            TrafDbDataReader dr = cmd.ExecuteReader();
            while (dr.Read())
            {
                Console.WriteLine("{0} {1}", dr[0], dr[1]);
            }
        }

        public static void TestAdapterFillParams()
        {
            TrafDbConnection conn = new TrafDbConnection(Program.ConnectionString);
            conn.Open();
            TrafDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "drop table t1";
            try { cmd.ExecuteNonQuery(); }
            catch { } //ignore error
            cmd.CommandText = "create table t1 (c1 int not null not droppable primary key, c2 int) no partition";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into t1 values(1,1),(2,1),(3,1),(4,1)";
            cmd.ExecuteNonQuery();

            Console.WriteLine("Setup Complete");

            TrafDbCommand selectCmd = conn.CreateCommand();
            selectCmd.CommandText = "select * from t1 where c1>3";
            //selectCmd.Parameters.Add(new TrafDbParameter("c1",TrafDbDbType.Integer));

            TrafDbDataAdapter adp = new TrafDbDataAdapter();
            adp.SelectCommand = selectCmd;

            DataTable dt = new DataTable();
            DataTable dt1 = new DataTable();

            //get all the values with c1 > 2
            TrafDbParameter [] p = adp.GetFillParameters();
            Console.WriteLine(p.Length);

            //display the data side by side or something
        }

        public static void FetchTest()
        {
            TrafDbConnection conn;
            TrafDbCommand cmd;
            TrafDbDataReader dr;
            DateTime start;
            DateTime end;
            long total;

            int iterations = 5;
            int[] fetchBufferSizes = { 4096 };
            int[] fetchSizes = { 100000 };
            TimeSpan[][] results = new TimeSpan[fetchBufferSizes.Length][];

            conn = new TrafDbConnection();
            cmd = conn.CreateCommand();
            cmd.CommandText = "select * from neo.doermann.t2";

            for (int i = 0; i < fetchBufferSizes.Length; i++)
            {
                results[i] = new TimeSpan[fetchSizes.Length];

                conn.ConnectionString = Program.ConnectionString; 
                conn.Open();
                Console.WriteLine(conn.RemoteProcess);

                for (int j = 0; j < fetchSizes.Length; j++)
                {
                    total = 0;
                    for (int k = 0; k < iterations; k++)
                    {
                        Console.Write(String.Format("{0} {1} {2}:   ", i, j, k));
                        dr = cmd.ExecuteReader();
                        //dr.FetchSize = fetchSizes[j];

                        start = DateTime.Now;
                        while (dr.Read())
                        {
                            dr.GetInt32(0);
                            dr.GetInt32(1);
                            dr.GetInt32(2);
                            dr.GetInt32(3);
                            dr.GetInt32(4);
                            dr.GetInt32(5);
                        }
                        end = DateTime.Now;

                        total += (end - start).Ticks;
                        Console.WriteLine((end - start).TotalMilliseconds);
                    }
                    results[i][j] = new TimeSpan(total / iterations);
                }

                conn.Close();
            }

            for (int i = 0; i < fetchBufferSizes.Length; i++)
            {
                for (int j = 0; j < fetchSizes.Length; j++)
                {
                    Console.WriteLine(String.Format("FetchBufferSize={0} MaxRows={1} Time={2}", fetchBufferSizes[i], fetchSizes[j], results[i][j]));
                }
            }
        }

        public static void TestFactory()
        {
            DataTable dt = DbProviderFactories.GetFactoryClasses();
            foreach (DataRow row in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    Console.WriteLine(col.ColumnName + ": " + row[col]);
                }
            }
        }

        public static void OnInfoMessage(object sender, TrafDbInfoMessageEventArgs args)
        {
            Console.WriteLine("INFO: " + args.Message);
            Console.WriteLine("Errors List: ");
            foreach (TrafDbError error in args.Errors)
            {
                Console.WriteLine(String.Format("\t{0} | {1} | {2} | {3}", error.ErrorCode, error.Message, error.RowId, error.State));
            }
        }

        public static void OnStateChange(object sender, StateChangeEventArgs args)
        {
            Console.WriteLine("State changed: " + args.OriginalState + " -> " + args.CurrentState);
        }

        public static void PrintErrors(TrafDbException e, bool includeStack) 
        {
            Console.WriteLine(e.Message);
            Console.WriteLine("Errors List: ");
            foreach (TrafDbError error in e.Errors)
            {
                Console.WriteLine(String.Format("\t{0} | {1} | {2} | {3}", error.ErrorCode, error.Message, error.RowId, error.State));
            }
            if (e.InnerException != null)
            {
                Console.WriteLine("InnerException: " + e.InnerException.Message);
            }
            if (includeStack)
            {
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}