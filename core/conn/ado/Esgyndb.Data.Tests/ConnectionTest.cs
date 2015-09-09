using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Net;

using System.Data.Odbc;

using NUnit.Framework;
using Esgyndb.Data;
using System.Configuration;

namespace TrafAdoTest
{
    [TestFixture]
    public class ConnectionTest
    {

        private string server = ConfigurationManager.AppSettings["HostName"];
        /*
        private string port = "18650";
        private string user = "qauser_user";
        private string password = "HPDb2009";
        private string catalog = "NEO";
        private string schema = "ADOQA_SCHEMA";
        */

        private string connectionString;

        [SetUp]
        public void SetUp()
        {
            /*
            connectionString = "Server=" + server + ":" + port +
                                ";User=" + user +
                                ";Password=" + password +
                                ";Catalog=" + catalog +
                                ";Schema=" + schema +
                                ";";
             */
            //        private string connectionString = "DSN=wmb_coast;UID=wm.super;PWD=wmsuper;";

            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            Console.WriteLine("connectionString = " + connectionString);
        }

        [Test]
        public void ConnectionWithParam()
        {
            connectionString += "logintimeout=120;";
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                //Assert.AreEqual(this.connectionString, conn.ConnectionString);
                //Assert.AreEqual(120, conn.ConnectionTimeout);
                //Assert.AreEqual("", conn.Container);
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.AreEqual("TDM_Default_DataSource", conn.DataSource);
                //Assert.AreEqual(1, conn.IsoMapping);
                Console.WriteLine("Just before GetHostEntry call");
                string hostname;
                if (this.server.Contains(":"))
                {
                    int len = this.server.Length - 6;
                    hostname = this.server.Substring(0, len);
                }
                else hostname = this.server;
                Console.WriteLine("Expected:" + Dns.GetHostEntry(hostname).HostName.Substring(0, 5) + " and Actual:" + Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 5));
                Assert.AreEqual(Dns.GetHostEntry(hostname).HostName.Substring(0, 5),
                                Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 5));
                //Assert.AreEqual("18650", conn.RemotePort);
                string serverProcessName = Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 7) + ".$";
                //Assert.IsTrue(conn.RemoteProcess.StartsWith(serverProcessName, StringComparison.OrdinalIgnoreCase));
                //Assert.AreEqual("", conn.ServerVersion);
                //Assert.AreEqual("", conn.Site);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                conn.Close();
            }
        }

        [Test]
        public void ConnectionWithNoParam()
        {
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = connectionString;
                //conn.ConnectionTimeout = 0;
                conn.Open();
                //Assert.AreEqual(this.connectionString, conn.ConnectionString);
                //Assert.AreEqual(120, conn.ConnectionTimeout);
                //Assert.AreEqual("", conn.Container);
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.AreEqual("TDM_Default_DataSource", conn.DataSource);
                //Assert.AreEqual(1, conn.IsoMapping);
                Console.WriteLine("Just before GetHostEntry call");

                string hostname;
                if (this.server.Contains(":"))
                {
                    int len = this.server.Length - 6;
                    hostname = this.server.Substring(0, len);
                }
                else hostname = this.server;

                Console.WriteLine("hostname : " + hostname);
                Console.WriteLine("conn.RemoteAddress : " + conn.RemoteAddress);

                Console.WriteLine("Dns.GetHostEntry(hostname) : " + Dns.GetHostEntry(hostname));
                Console.WriteLine("Dns.GetHostEntry(conn.RemoteAddress) : " + Dns.GetHostEntry(conn.RemoteAddress));

                Console.WriteLine("Expected: " + Dns.GetHostEntry(hostname).HostName.Substring(0, 5));
                Console.WriteLine("Actual: " + Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 5));
                Assert.AreEqual(Dns.GetHostEntry(hostname).HostName.Substring(0, 5),
                                Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 5));
                //Assert.AreEqual("18650", conn.RemotePort);
                string serverProcessName = Dns.GetHostEntry(conn.RemoteAddress).HostName.Substring(0, 7) + ".$";
                //Assert.IsTrue(conn.RemoteProcess.StartsWith(serverProcessName, StringComparison.OrdinalIgnoreCase));
                //Assert.AreEqual("", conn.ServerVersion);
                //Assert.AreEqual("", conn.Site);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                conn.Close();
            }
        }

        [Test]
        [ExpectedException(typeof(CommunicationsFailureException))]
        public void ConnectionWithNoConnectionStringNeg()
        {
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.Open();
            }
        }

        [Test]
        public void ConnectionWithInvalidLoginsNeg()
        {
            string[] invalidUID =  {
                                  "",
                                  " ",
                                  "ab c",
                                  "@#!$%^&",
                                  "notusedyet"
            };

            string[] invalidPWD = {   
                                  "",
                                  " ",
                                  "ab c",
                                  "@#!$%^&",
                                  "notusedyet"
            };

        //    //Negative test: Invalid userid
        //    foreach (string uid in invalidUID)
        //    {
        //        try
        //        {
        //            string connString = "server=" + server + ":" + port +
        //                                ";user=" + uid +
        //                                ";password=" + password +
        //                                ";catalog=" + catalog +
        //                                ";schema=" + schema + ";";

        //            using (EsgyndbConnection conn = new EsgyndbConnection(connString))
        //            {
        //                Console.WriteLine(conn.ConnectionString);
        //                conn.Open();
        //                conn.Close();
        //               // Assert.
        //            }
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message.Equals(msg1) || e.Message.Contains(msg2))
        //                testInfo.Passed();
        //            else
        //            {
        //                testInfo.Failed();
        //                Helper.DisplayMessage(Helper.msgType.ERR, "Expect: " + msg1 + "\r\nActual: " + e.Message);
        //            }
        //        }
        //    }

        //    //Negative test: Valid userid, Invalid password
        //    foreach (string pwd in invalidPWD)
        //    {
        //        try
        //        {
        //            string connectionString = Login.getConnectionString(this.dsn, this.username, pwd);
        //            using (OdbcConnection conn = new OdbcConnection(connectionString))
        //            {
        //                Console.WriteLine(conn.ConnectionString);
        //                conn.Open();
        //                conn.Close();
        //            }
        //            testInfo.Failed();
        //        }
        //        catch (Exception e)
        //        {
        //            if (e.Message.Equals(msg1) || e.Message.Contains(msg2))
        //                testInfo.Passed();
        //            else
        //            {
        //                testInfo.Failed();
        //                Helper.DisplayMessage(Helper.msgType.ERR, "Expect: " + msg1 + "\r\nActual: " + e.Message);
        //            }
        //        }
        //    }
        }

        [Test]
        public void GetSchema()
        {
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                System.Data.DataTable dt = conn.GetSchema("Tables", new string[]{"NEO","ADOQA_SCHEMA"});
                foreach (System.Data.DataRow row in dt.Rows)
                {
                    foreach (System.Data.DataColumn col in dt.Columns)
                    {
                        Console.WriteLine("{0} = {1}", col.ColumnName, row[col]);
                    }
                    Console.WriteLine("============================");
                }
                conn.Close();
            }
        }

        [Test]
        public void GetSchema_none()
        {
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                System.Data.DataTable dt = conn.GetSchema();
                foreach (System.Data.DataRow row in dt.Rows)
                {
                    foreach (System.Data.DataColumn col in dt.Columns)
                    {
                        Console.WriteLine("{0} = {1}", col.ColumnName, row[col]);
                    }
                    Console.WriteLine("============================");
                }
                conn.Close();
            }
        }

        [Test]
        public void GetSchema_schemas()
        {
            //String connstr = connectionString + "IdleTimeout=1;";
            String connstr = connectionString;

            using (EsgyndbConnection conx = new EsgyndbConnection(connstr))
            {
                conx.Open();
                System.Data.DataTable dt = conx.GetSchema("Schemas", new string[] { "NEO"});
                foreach (System.Data.DataRow row in dt.Rows)
                {
                    foreach (System.Data.DataColumn col in dt.Columns)
                    {
                        Console.WriteLine("{0} = {1}", col.ColumnName, row[col]);
                    }
                    Console.WriteLine("============================");
                }
                conx.Close();
            }
        }

        [Test]
        [ExpectedException(typeof(CommunicationsFailureException))]
        public void IdleTimeout()
        {
            String connstr = connectionString + "IdleTimeout=10;";
            EsgyndbConnection conx = new EsgyndbConnection(connstr);
            conx.Open();
            EsgyndbCommand tstcmd = new EsgyndbCommand();
            tstcmd.Connection = conx;

            //tstcmd.CommandText = "SELECT COUNT(*) FROM ADONET_TABLE A INNER JOIN ADONET_TABLE B ON A.C19=B.C19";
            tstcmd.CommandText = "SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ADONET_TABLE UNION ALL SELECT COUNT(*) FROM ADONET_TABLE";

            //tstcmd.Prepare();

            //System.Threading.Thread.Sleep(5000);

            //object c = tstcmd.ExecuteScalar();

            EsgyndbDataReader tstrdr = tstcmd.ExecuteReader();

            System.Threading.Thread.Sleep(10000);

            Assert.IsTrue(tstrdr.HasRows);
            //Assert.AreNotEqual(Convert.ToInt32(c), 0);

            tstrdr.Close();
            tstcmd.Dispose();
            conx.Close();
        }

        [Test]
        public void CreateCommand()
        {
            EsgyndbConnection conn = new EsgyndbConnection(connectionString);
            conn.Open();
            EsgyndbCommand tstcmd =  conn.CreateCommand();

            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);
            Console.WriteLine("Label : " + tstcmd.Label);
            //Assert.AreEqual("TSTLBL", tstcmd.Label);

            tstcmd.Dispose();
            conn.Close();
        }

        [Test]
        public void CreateCommand_label()
        {
            /*
            EsgyndbConnection conn = new EsgyndbConnection(connectionString);
            conn.Open();
            EsgyndbCommand tstcmd = conn.CreateCommand("TSTLBL");

            Console.WriteLine("CommandTimeout : " + tstcmd.CommandTimeout);
            Console.WriteLine("Label : " + tstcmd.Label);
            Assert.AreEqual("TSTLBL", tstcmd.Label);

            tstcmd.Dispose();
            conn.Close();
             * */
        }
    }
}
