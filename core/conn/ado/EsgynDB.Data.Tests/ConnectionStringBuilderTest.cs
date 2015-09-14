using System;
using System.Data;
using System.Collections.Generic;
using System.Text;
using System.Collections;

using NUnit.Framework;
using EsgynDB.Data;
using System.Configuration;

namespace TrafAdoTest
{
    [TestFixture]
    class ConnectionStringBuilderTest
    {
        //TestObject testInfo = AdoTestSetup.testInfo;
        public EsgynDBConnectionStringBuilder connStrBuilder;
        public EsgynDBConnection conn;

        private string server = ConfigurationManager.AppSettings["HostName"];

        [SetUp]
        public void SetUp()
        {

        }

        [TearDown]
        public void TearDown()
        {

        }

        //Test data, it may not include the all keys
        public Dictionary<string, string> TestDic()
        {
            Dictionary<String, String> D = new Dictionary<string, string>();
            //D.Add("Server", "sqa0101.cup.hp.com");
            D.Add("Server", server);
            D.Add("Schema", "ADOQA_SCHEMA");
            D.Add("LoginTimeout", "30");
            D.Add("Catalog", "NEO");
            D.Add("Datasource", "TDM_Default_DataSource");
            D.Add("RetryCount", "6");
            D.Add("IdleTimeout", "60");
            D.Add("User", "sql_user");
            D.Add("Password", "redhat06");
            D.Add("Rolename", "ROLE.USER");
            //D.Add("MaxPoolSize", "10");
            return D;
        }

        public EsgynDBConnectionStringBuilder BuildTestString(Dictionary<String, String> D)
        {
            connStrBuilder = new EsgynDBConnectionStringBuilder();

            foreach (String key in D.Keys)
            {
                connStrBuilder.Add(key, D[key]);
            }
            return connStrBuilder;
        }

        //Negative test data
        public List<String> negTestData()
        {
            List<String> negData = new List<String>();
            negData.Add("");
            negData.Add("abdce");
            negData.Add(";Schema=");
            return negData;
        }

        //test data, include the all keys
        public Dictionary<string, string> structData()
        {
            Dictionary<string, string> structD = new Dictionary<string, string>();
            structD.Add("Server",server);
            structD.Add("Datasource", "TDM_Default_DataSource");
            structD.Add("Catalog", "NEO");
            structD.Add("Schema", "ADOQA_SCHEMA");
            structD.Add("User", "qauser_user");
            structD.Add("Password", "Neoview2009");
            structD.Add("Rolename", "ROLE.USER");
            structD.Add("RetryCount", "10");         //default value 3
            structD.Add("RetryTime", "6000");        //default value 5000
            structD.Add("TcpKeepAlive", "True");     //default value true
            structD.Add("IdleTimeout", "1000");      //default value 600
            structD.Add("LoginTimeout", "1000");     //default value 300
            structD.Add("FetchBufferSize", "8192");  //default value 4096
            //MAR 9, 2011 - ARUNA - CpuToUse FAILS IN SQ
            structD.Add("CpuToUse", "0");            //default value -1
            structD.Add("ApplicationName", "nunit");
            structD.Add("MaxPoolSize", "10");
            return structD;
        }

        [Test]
        public void Remove()
        {
            String connStr;
            String restStr;
            Dictionary<String, String> D = TestDic();
            connStrBuilder = BuildTestString(D);
            bool actReturn;
           
            //Positive test
            foreach (String remvStr in D.Keys)
            {   
                //get the expected string which has been removed the specified key
                connStr = connStrBuilder.ConnectionString;
                int index1 = connStr.IndexOf(remvStr);
                int index2 = connStr.IndexOf(';', index1);
                restStr = connStr.Remove(index1, index2 - index1 + 1);

                //get the actual connectionString and compare
                actReturn = connStrBuilder.Remove(remvStr);
                Console.WriteLine("Expected: " + restStr);
                Console.WriteLine("Actual: " + connStrBuilder.ConnectionString);
                Assert.AreEqual(restStr, connStrBuilder.ConnectionString);
                Assert.AreEqual(true, actReturn);              
            }
        }

        [Test]
        public void RemoveNeg()
        {
            String connStr;
            Dictionary<String, String> D = TestDic();
            connStrBuilder = BuildTestString(D);
            List<String> remvStrList = new List<String>();
            bool actReturn;

            remvStrList = negTestData();

            //Negative test
            foreach (String remvStr in remvStrList)
            {   
                //the specified key can not be removed because it do not exist in the connectionString
                connStr = connStrBuilder.ConnectionString;
                actReturn = connStrBuilder.Remove(remvStr);
                //the ConnectionString should be the same as the one before removing
                Assert.AreEqual(connStr, connStrBuilder.ConnectionString);
                Assert.AreEqual(false, actReturn);
            }
        }

        [Test]
        public void ShouldSerialize()
        {
            Dictionary<String, String> D = TestDic();
            connStrBuilder = BuildTestString(D);
            bool expResult, actResult;

            //Positive test
            foreach (String serializeStr in D.Keys) 
            {
                expResult = true;
                actResult = connStrBuilder.ShouldSerialize(serializeStr);
                Assert.AreEqual(expResult, actResult);
                Console.WriteLine(serializeStr + ": " + expResult.ToString() + " vs " + actResult.ToString());
            }
        }

        [Test]
        public void ShouldSerializeNeg()
        {
            List<String> serializeStrList = new List<string>();
            bool expResult, actResult;

            serializeStrList = negTestData();
            //the key FetchBufferSize is in the dictionary but not in the connectionString
            //the difference between ShouldSerialize() and ContainsKey()
            serializeStrList.Add("FetchBufferSize");

            //Negative test
            foreach (String serializeStr in serializeStrList)
            {
                expResult = false;
                actResult = connStrBuilder.ShouldSerialize(serializeStr);
                Assert.AreEqual(expResult, actResult);
                Console.WriteLine(serializeStr + ": " + expResult.ToString() + " vs " + actResult.ToString());
            }
        }

        [Test]
        public void ContainsKey()
        {   
            //using structData() to construct the test data including all keys
            Dictionary<String, String> D = structData();
            connStrBuilder = BuildTestString(D);
            bool expResult, actResult;

            Console.WriteLine("*****" + connStrBuilder.ConnectionString);
            
            //Positive test
            foreach (String containsStr in D.Keys)
            {
                expResult = true;
                actResult = connStrBuilder.ContainsKey(containsStr);
                Assert.AreEqual(expResult, actResult);
                Console.WriteLine(containsStr + ": " + expResult.ToString() + " vs " + actResult.ToString());
            }
        }

        [Test]
        public void ContainsKeyNeg()
        {
            List<String> negStrList = new List<string>();
            bool expResult, actResult;

            negStrList = negTestData();

            //Negative test
            foreach (String containsStr in negStrList)
            {
                expResult = false;
                actResult = connStrBuilder.ShouldSerialize(containsStr);
                Assert.AreEqual(expResult, actResult);
                Console.WriteLine(containsStr + ": " + expResult.ToString() + " vs " + actResult.ToString());
            }
        }

        [Test]
        public void TryGetValue()
        {
            Dictionary<String, String> D = TestDic();
            connStrBuilder = BuildTestString(D);
            String expResult;
            Object actResult;
            bool expReturn, actReturn;

            //Positive test
            foreach (String getValue in D.Keys)
            {
                expReturn = D.TryGetValue(getValue, out expResult);
                actReturn = connStrBuilder.TryGetValue(getValue, out actResult);
                if (expReturn)
                {
                    Console.WriteLine(expResult + " vs " + actResult);
                    Console.WriteLine(expReturn + " vs " + actReturn);
                    Assert.AreEqual(expResult, actResult);
                    Assert.AreEqual(expReturn, actReturn);
                }
            }
        }

        [Test]
        public void TryGetValueNeg()
        {
            List<String> negStrList = new List<string>();
            //String expResult;
            Object actResult;
            bool expReturn, actReturn;

            negStrList = negTestData();

            //Negative test
            foreach (String getValue in negStrList)
            {
                expReturn = false;
                actReturn = connStrBuilder.TryGetValue(getValue, out actResult);
                Console.WriteLine("False" + " vs " + actReturn);
                Assert.AreEqual(expReturn, actReturn);
            }

        }

        [Test]
        public void Clear()
        {
            Dictionary<String, String> D = TestDic();
            
            connStrBuilder = new EsgynDBConnectionStringBuilder();
            String expResult = connStrBuilder.ConnectionString;
            Console.WriteLine("Expected: " + expResult);
            
            //build the connectionString for clear
            connStrBuilder = BuildTestString(D);
            Console.WriteLine("After building: " + connStrBuilder.ConnectionString);

            if (connStrBuilder.ConnectionString.Length > expResult.Length)
            {
                connStrBuilder.Clear();
                String actResult = connStrBuilder.ConnectionString;
                Console.WriteLine("Actual: " + actResult);
                Assert.AreEqual(expResult, actResult);
            }
            else
            {
                throw new Exception(string.Format("Please build a new connection string!"));
            }
        }

        //test for Constructor with String argument
        //MAR 9, 2011 - ARUNA - CpuToUse FAILS IN SQ
        [Test]
        public void EsgyndbConnectionStringBuilderWithArgument()
        {
            //String connString = "Server=wmb0101;Datasource=TFM;Catalog=NEO;Schema=ADOQA_SCHEMA;User=odbcqa;Password=odbcqa;Rolename=user;RetryCount=1000;RetryTime=1000;TcpKeepAlive=True;IdleTimeout=1000;LoginTimeout=1000;FetchBufferSize=1000;CpuToUse=1000;ApplicationName=nunit";
            //create the connString
            String connString = "";
            Dictionary<String, String> D = structData();
            int i=1;
            
            foreach (String key in D.Keys)
            {
                if (i == D.Count && D[key] != "")
                {
                    connString += key + "=" + D[key];   
                }
                else if (i == D.Count && D[key] == "")
                {
                    connString = connString.Remove(connString.Length-1);     
                }
                else if (i < D.Count && D[key] != "")
                {
                    connString += key + "=" + D[key] + ";";
                }               
                i++;
            }

            Console.WriteLine("**************** THIS TEST FAILS IN SQ IF CONNECTION STRING HAS CPUTOUSE *******************");
            Console.WriteLine(connString);
            connStrBuilder = new EsgynDBConnectionStringBuilder(connString);
            conn = new EsgynDBConnection(connStrBuilder.ConnectionString);
            conn.Open();
            if (conn.State == ConnectionState.Open)
            {
                Console.WriteLine("Connection has been established using the string created by NeoveiwConnectionStringBuilder!");
                conn.Close();
                if (conn.State == ConnectionState.Closed)
                {
                    Console.WriteLine("Then the connection is closed!");
                }
            }
            else
            {
                Console.WriteLine("connection has not been established! ");
            }

            //if (!connString.Contains("ApplicationName"))
            //{
            //    connStrBuilder.Remove("ApplicationName");
            //}
            //Console.WriteLine(connStrBuilder.ConnectionString);
            //Assert.AreEqual(connString, connStrBuilder.ConnectionString);
        }
        
        //test for Constructor without argument
        [Test]
        public void EsgyndbConnectionStringBuilder()
        {
            connStrBuilder = new EsgynDBConnectionStringBuilder();
            Console.WriteLine(connStrBuilder.ConnectionString);
            // SEAQUEST Assert.AreEqual("ApplicationName=nunit.exe", connStrBuilder.ConnectionString);
            
            connStrBuilder.Add("Server", server);
            connStrBuilder.Add("Schema", "ADOQA_SCHEMA");
            connStrBuilder.Add("Catalog", "NEO");
            connStrBuilder.Add("Datasource", "TDM_Default_DataSource");
            connStrBuilder.Add("User", "qauser_user");
            connStrBuilder.Add("Password", "Neoview2009");
            connStrBuilder.Add("MaxPoolSize", "-1");

            Console.WriteLine(connStrBuilder.ConnectionString);
            conn = new EsgynDBConnection(connStrBuilder.ConnectionString);
            conn.Open();
            if (conn.State == ConnectionState.Open)
            {
                Console.WriteLine("Connection has been established using the string created by NeoveiwConnectionStringBuilder!");
                conn.Close();
                if (conn.State == ConnectionState.Closed)
                {
                    Console.WriteLine("Then the connection is closed!");
                }
            }
            else
            {
                Console.WriteLine("connection has not been established! ");
            }
        }

        //test for all the properties
        [Test]
        public void TestProperty()
        {
            //***test 1: set the value of the members using ADD()
            Dictionary<String, String> D = structData();
            connStrBuilder = BuildTestString(D);

            //Console.WriteLine(D["Server"] + " vs " + connStrBuilder.Server);
            //Console.WriteLine(D["Datasource"] + " vs " + connStrBuilder.Datasource);
            //Console.WriteLine(D["Catalog"] + " vs " + connStrBuilder.Catalog);
            //Console.WriteLine(D["Schema"] + " vs " + connStrBuilder.Schema);
            //Console.WriteLine(D["User"] + " vs " + connStrBuilder.User);
            //Console.WriteLine(D["Password"] + " vs " + connStrBuilder.Password);
            //Console.WriteLine(D["Rolename"] + " vs " + connStrBuilder.Rolename);
            //Console.WriteLine(D["RetryCount"] + " vs " + connStrBuilder.RetryCount);
            //Console.WriteLine(D["RetryTime"] + " vs " + connStrBuilder.RetryTime);
            //Console.WriteLine(D["TcpKeepAlive"] + " vs " + connStrBuilder.TcpKeepAlive);
            //Console.WriteLine(D["IdleTimeout"] + " vs " + connStrBuilder.IdleTimeout);
            //Console.WriteLine(D["FetchBufferSize"] + " vs " + connStrBuilder.FetchBufferSize);
            //Console.WriteLine(D["CpuToUse"] + " vs " + connStrBuilder.CpuToUse);
            //Console.WriteLine(D["ApplicationName"] + " vs " + connStrBuilder.ApplicationName); 

            Assert.AreEqual(D["Server"], connStrBuilder.Server);
            Assert.AreEqual(D["Datasource"], connStrBuilder.Datasource);
            Assert.AreEqual(D["Catalog"], connStrBuilder.Catalog);
            Assert.AreEqual(D["Schema"], connStrBuilder.Schema);
            Assert.AreEqual(D["User"], connStrBuilder.User);
            Assert.AreEqual(D["Password"], connStrBuilder.Password);
            Assert.AreEqual(D["Rolename"], connStrBuilder.Rolename);
            Assert.AreEqual(D["RetryCount"], connStrBuilder.RetryCount.ToString());
            Assert.AreEqual(D["RetryTime"], connStrBuilder.RetryTime.ToString());
            Assert.AreEqual(D["TcpKeepAlive"], connStrBuilder.TcpKeepAlive.ToString());
            Assert.AreEqual(D["IdleTimeout"], connStrBuilder.IdleTimeout.ToString());
            Assert.AreEqual(D["FetchBufferSize"], connStrBuilder.FetchBufferSize.ToString());
            Assert.AreEqual(D["CpuToUse"], connStrBuilder.CpuToUse.ToString());
            // SEAQUEST Assert.AreEqual(D["ApplicationName"], connStrBuilder.ApplicationName);
            Assert.AreEqual(D["MaxPoolSize"], connStrBuilder.MaxPoolSize.ToString());


            //***test 2: reset the value of the members using assign symbol
            connStrBuilder.Server = "aaa";
            connStrBuilder.Datasource = "bbb";
            connStrBuilder.Catalog = "ccc";
            connStrBuilder.Schema = "ddd";
            connStrBuilder.User = "eee";
            connStrBuilder.Password = "fff";
            connStrBuilder.Rolename = "ggg";
            connStrBuilder.RetryCount = 100;
            connStrBuilder.RetryTime = 200;
            connStrBuilder.TcpKeepAlive = false;
            connStrBuilder.IdleTimeout = 300;
            connStrBuilder.FetchBufferSize = 400;
            connStrBuilder.CpuToUse = 500;
            connStrBuilder.ApplicationName = "hhh";
            connStrBuilder.MaxPoolSize = 0;

            Assert.AreEqual("aaa", connStrBuilder.Server);
            Assert.AreEqual("bbb", connStrBuilder.Datasource);
            Assert.AreEqual("ccc", connStrBuilder.Catalog);
            Assert.AreEqual("ddd", connStrBuilder.Schema);
            Assert.AreEqual("eee", connStrBuilder.User);
            Assert.AreEqual("fff", connStrBuilder.Password);
            Assert.AreEqual("ggg", connStrBuilder.Rolename);
            Assert.AreEqual(100, connStrBuilder.RetryCount);
            Assert.AreEqual(200, connStrBuilder.RetryTime);
            Assert.AreEqual(false, connStrBuilder.TcpKeepAlive);
            Assert.AreEqual(300, connStrBuilder.IdleTimeout);
            Assert.AreEqual(400, connStrBuilder.FetchBufferSize);
            Assert.AreEqual(500, connStrBuilder.CpuToUse);
            Assert.AreEqual("hhh", connStrBuilder.ApplicationName);
            Assert.AreEqual("0", connStrBuilder.MaxPoolSize.ToString());

            Console.WriteLine(connStrBuilder.ConnectionString);

        }
    }
}
