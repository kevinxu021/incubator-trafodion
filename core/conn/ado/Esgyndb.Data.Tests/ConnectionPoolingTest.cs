using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Data.Odbc;
using NUnit.Framework;
using Esgyndb.Data;
using System.Configuration;
using System.Diagnostics;
using System.Threading;

namespace TrafAdoTest
{
    [TestFixture]
    public class ConnectionPoolingTest
    {
        private string server = ConfigurationManager.AppSettings["HostName"];
        private string connectionString;

        [SetUp]
        public void SetUp()
        {
            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
        }

        [Test]
        public void SimpConnectionWithPoolingEnabled()
        {
            string serverProcessName;
            int serverPort;
            //connectionString += "logintimeout=120;";
            connectionString += "MaxPoolSize=10;";
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                serverProcessName = conn.RemoteProcess;
                serverPort = conn.RemotePort;
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));
            }
            //HPDM SHOWS SERVER AS AVAILABLE AT THIS POINT, SHOULD BE CONNECTED. - FIXED BY MATT ON 10/12/2010
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                serverProcessName = conn.RemoteProcess;
                serverPort = conn.RemotePort;
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));
            }
        }

        [Test]
        public void ConnectionWithPoolingEnabledChangeConnectionString()
        {
            string serverProcessName;
            int serverPort;
            connectionString += "MaxPoolSize=10;";
            EsgyndbConnection conn = new EsgyndbConnection(connectionString);
            conn.Open();

            Assert.AreEqual("TRAFODION", conn.Database);
            serverProcessName = conn.RemoteProcess;
            serverPort = conn.RemotePort;
            Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
            conn.Close();
            Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

            connectionString += "logintimeout=120;";
            EsgyndbConnection conn1 = new EsgyndbConnection(connectionString);
            conn1.Open();
            Assert.AreEqual("TRAFODION", conn1.Database);
            Assert.IsTrue(conn1.State.Equals(ConnectionState.Open));
            Assert.AreNotEqual(serverProcessName, conn1.RemoteProcess);
            Assert.AreNotEqual(serverPort, conn1.RemotePort);
            conn1.Close();
            Assert.IsTrue(conn1.State.Equals(ConnectionState.Closed));
        }

        [Test]
        public void ConnectionWithPoolingEnabled()
        {
            string serverProcessName;
            int serverPort;
            //connectionString += "logintimeout=120;";
            connectionString += "MaxPoolSize=10;";
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                serverProcessName = conn.RemoteProcess;
                serverPort = conn.RemotePort;
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));

                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                conn.Close();
                Assert.IsTrue(conn.State.Equals(ConnectionState.Closed));
            }

            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.AreEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreEqual(serverPort, conn.RemotePort);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));

                conn.Close();
            }

            connectionString = ConfigurationManager.ConnectionStrings[ConfigurationManager.AppSettings["ConnectionStringName"]].ConnectionString;
            connectionString += "MaxPoolSize=-1;";
            connectionString += "ApplicationName=nunit.exe";
            using (EsgyndbConnection conn = new EsgyndbConnection(connectionString))
            {
                conn.Open();
                Assert.AreEqual("TRAFODION", conn.Database);
                Assert.AreNotEqual(serverProcessName, conn.RemoteProcess);
                Assert.AreNotEqual(serverPort, conn.RemotePort);
                Assert.IsTrue(conn.State.Equals(ConnectionState.Open));

                conn.Close();
            }
        }

        // verify that getting a pooled connection is faster than a regular connection
        [Test]
        public void ConnectionPoolingTestTime()
        {
            Console.WriteLine("\n\n**TestTime**");

            Stopwatch sw = new Stopwatch();
            string str = connectionString + ";MaxPoolSize=1";

            // regular connection
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = str;
                sw.Start();
                conn.Open();
                sw.Stop();
                conn.Close();
            }

            Console.WriteLine("regular connection took: {0}", sw.ElapsedMilliseconds);
            sw.Reset();

            // pooled connection
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = str;
                sw.Start();
                conn.Open();
                sw.Stop();
                conn.Close();
            }

            Console.WriteLine("pooled connection took: {0}", sw.ElapsedMilliseconds);

            if (sw.ElapsedMilliseconds > 200)
            {
                throw new Exception("test failed!  getting a pooled connection should be extremely fast!");
            }
        }

        // verify that only X number of connections are saved and that they are returned in the proper order
        [Test]
        public void ConnectionPoolingTestPool()
        {
            Console.WriteLine("\n\n**TestPool**");

            EsgyndbConnection[] conns = new EsgyndbConnection[5];
            string[] p = new string[5];

            // create 5 connections
            for (int i = 0; i < conns.Length; i++)
            {
                conns[i] = new EsgyndbConnection();
                conns[i].ConnectionString = connectionString + ";MaxPoolSize=2";
                conns[i].Open();

                p[i] = conns[i].RemoteProcess;
                Console.WriteLine(p[i]);
            }

            Console.WriteLine("");

            // close all 5 connections, only MOST RECENT 2 will be saved since MaxPoolSize=2
            for (int i = 0; i < conns.Length; i++)
            {
                conns[i].Close();
            }

            // get 5 connections again, the first 2 connections should come from the pool (#4 and #5 from the previous run)
            //  and the last 3 connections should be random
            for (int i = 0; i < conns.Length; i++)
            {
                conns[i] = new EsgyndbConnection();
                conns[i].ConnectionString = connectionString + ";MaxPoolSize=2";
                conns[i].Open();
                Console.WriteLine(conns[i].RemoteProcess);
            }

            // verify that the proper connections were saved and retrieved
            if (conns[0].RemoteProcess != p[3] || conns[1].RemoteProcess != p[4])
            {
                throw new Exception("test failed!  the first two connections of the 2nd run should match the last two connections of the first run!");
            }
        }

        [Test]
        public void ConnectionPoolingTestIdle()
        {
            Console.WriteLine("\n\n**TestIdle**");

            string str = connectionString + ";MaxPoolSize=1;IdleTimeout=10";
            Console.WriteLine(str);
            // regular connection
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = str;
                conn.Open();
                conn.Close();
            }

            // connection will die after 10 seconds
            //Thread.Sleep(15000);

            // will first attempt to give the pooled connection, realize it is dead, and create a regular connection instead
            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = str;
                conn.Open();
                conn.BeginTransaction().Rollback(); //make sure the connection is alive
                conn.Close();
            }

            Console.WriteLine("success");
        }

        // verify multiple pools
        [Test]
        public void ConnectionPoolingTestMultiple()
        {
            Console.WriteLine("\n\n**TestMultiple**");

            string str = connectionString + ";MaxPoolSize=1";
            string process;

            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                conn.ConnectionString = str;
                conn.Open();

                //save the process name
                process = conn.RemoteProcess;
                Console.WriteLine(conn.RemoteProcess);
                conn.Close();
            }

            using (EsgyndbConnection conn = new EsgyndbConnection())
            {
                //change a value
                conn.ConnectionString = str + ";logintimeout=120";
                conn.Open();
                Console.WriteLine(conn.RemoteProcess);
                conn.Close();

                if (conn.RemoteProcess == process)
                {
                    throw new Exception("test failed!  got a pooled connection back with a different conn string!");
                }
            }
        }
    }
}
