using System;
using System.Data;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;
using EsgynDB.Data;
using System.Configuration;

namespace TrafAdoTest
{
    [TestFixture]
    class ParameterCollectionTest
    {
        public EsgynDBParameterCollection ParmCol;
        //public EsgynDBParameter[] Array;
        public EsgynDBParameter[] Array = new EsgynDBParameter[10];
        public EsgynDBParameter parm;
        public String connectionString;
        public EsgynDBConnection conn;
        public EsgynDBCommand cmd;
        /*
        private string server = "sqa0101.cup.hp.com";
        private string port = "18650";
        private string user = "qauser_user";
        private string password = "HPDb2009";
        private string catalog = "NEO";
        private string schema = "ADOQA_SCHEMA";
        */
      
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

            conn = new EsgynDBConnection(connectionString);
            cmd = conn.CreateCommand();
            //parm = cmd.CreateParameter();
            conn.Open();
        }

        [TearDown]
        public void TearDown()
        {
            //if (ParmCol!=null)
              //  ParmCol.Dispose();
         
            if (conn.State == ConnectionState.Open)
            {
                conn.Close();
            }
        }

        /*
        [Test]
        public void ParameterCollection()
        {
           ParmCol = new EsgynDBParameterCollection(cmd);
           Console.WriteLine("ParmCol size Is: " + ParmCol.Count);
           Assert.AreEqual(0, ParmCol.Count);
        }
        */

        [Test]
        public void add()
        {
            parm = new EsgynDBParameter("p1", EsgynDBType.Char);
            int i = cmd.Parameters.Add(parm);
            Console.WriteLine("the Add return number is :{0}, the parmcol count is : {1}", i, cmd.Parameters.Count);
            Assert.AreEqual(0, i);
            Assert.AreEqual(1, cmd.Parameters.Count);
            Assert.AreEqual("p1", cmd.Parameters[0].ParameterName);
            Assert.AreEqual(EsgynDBType.Char, cmd.Parameters[0].EsgynDBDbType);
            EsgynDBParameter parmTmp = new EsgynDBParameter("p2", EsgynDBType.Date);
            i = cmd.Parameters.Add(parmTmp);
            Console.WriteLine("the Add return number is :{0}, the parmcol count is : {1}", i, cmd.Parameters.Count);
            Assert.AreEqual(1, i);
            Assert.AreEqual(2, cmd.Parameters.Count);
            Assert.AreEqual("p2", cmd.Parameters[1].ParameterName);
            Assert.AreEqual(EsgynDBType.Date, cmd.Parameters[1].EsgynDBDbType);  
        }

        [Test]
        public void AddRange()
        {
            EsgynDBParameter parm1 = new EsgynDBParameter("p1", EsgynDBType.Char);
            EsgynDBParameter parm2 = new EsgynDBParameter("p2", EsgynDBType.Date);

            EsgynDBParameter[] range = new EsgynDBParameter[2] {parm1, parm2};

            //Console.WriteLine(range);
            //ParmCol = new EsgynDBParameterCollection(cmd);
            ParmCol = cmd.Parameters;
            ParmCol.AddRange(range);
            Console.WriteLine("ParmCol's count : {0}", ParmCol.Count);
            Assert.AreEqual(range.Length, ParmCol.Count);
            for (int i = 0; i < ParmCol.Count; i++)
            {
                Console.WriteLine("range{0}'s parametername:{1}, DbType: {2}", i, range[i].ParameterName, range[i].DbType);
                Assert.AreEqual(range[i].DbType, ParmCol[i].DbType);
                Assert.AreEqual(range[i].ParameterName, ParmCol[i].ParameterName);
            }
        }

        [Test]
        public void AddRangeEn()
        {
            cmd.Connection = conn;
            try
            {
                cmd.CommandText = "drop table Region";
                cmd.ExecuteNonQuery();
            }
            catch { }

            cmd.CommandText = "create table Region (RegionID int not null not droppable, RegionDescription char(100), primary key(RegionID))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "INSERT INTO Region VALUES (?,?)";

            EsgynDBParameter parm1 = new EsgynDBParameter("p1", EsgynDBType.Integer);
            EsgynDBParameter parm2 = new EsgynDBParameter("p2", EsgynDBType.Char, 100);

            EsgynDBParameter[] range = new EsgynDBParameter[2] { parm1, parm2 };

            //Console.WriteLine(range);
            //ParmCol = new EsgynDBParameterCollection(cmd);
            ParmCol = cmd.Parameters;
            ParmCol.AddRange(range);
            Console.WriteLine("ParmCol's count : {0}", ParmCol.Count);
            Assert.AreEqual(range.Length, ParmCol.Count);
            for (int i = 0; i < ParmCol.Count; i++)
            {
                Console.WriteLine("range{0}'s parametername:{1}, DbType: {2}", i, range[i].ParameterName, range[i].DbType);
                Assert.AreEqual(range[i].DbType, ParmCol[i].DbType);
                Assert.AreEqual(range[i].ParameterName, ParmCol[i].ParameterName);
            }
        }

        public void GenerateParmData()
        {
            if (ParmCol != null)
                ParmCol.Clear();
            ParmCol = cmd.Parameters;
            int returnV = 0;
            int ParmCount = 0;
            const int Count = 5;
            for (int i = 0; i < Count; i++)
            {
                String name = "P" + i;
                Object value = i;
                EsgynDBParameter parmtmp = new EsgynDBParameter(name, i);
             //   array[i] = parmtmp;
                returnV = ParmCol.Add(parmtmp);
                ParmCount = returnV + 1;
            }
            Console.WriteLine("the ParmCol's count is: " + ParmCount);
           
            Assert.AreEqual(Count, ParmCount);
        }

        public void GenerateArray(int count,String name)
        {
            //EsgynDBParameter[] array = new EsgynDBParameter[count];
            for (int i = 0; i < count; i++)
            {
                String parmname = name + i;
                Object value = i;
                EsgynDBParameter parmtmp = new EsgynDBParameter(parmname, i);
                Array[i] = parmtmp;
            }
            //return array;
        }

        [Test]
        public void ContainsObject()
        //Indicates whether a DbParameter with the specified Value is contained in the collection
        {
            Object[] NegValue = new Object[2] { "negIs", 30 };
            GenerateParmData();
            foreach (EsgynDBParameter parmtmp in ParmCol)
            {
                //Object value = parmtmp.Value;
                //Console.WriteLine("Value is: " + value.ToString());
                //bool returnB = ParmCol.Contains(value);
                bool returnB = ParmCol.Contains(parmtmp);
                Assert.AreEqual(true, returnB);
            }
            foreach (Object tmp in NegValue)
            {
                EsgynDBParameter PTmp = new EsgynDBParameter("P",tmp);
                bool returnB = ParmCol.Contains(PTmp);
                Assert.AreEqual(false, returnB);
            }
        }

        [Test]
        public void ContainsString()
        //Indicates whether a DbParameter with the specified name exists in the collection.
        {
            String[] NegName = new String[3] { "12", "PP", "***" };
            GenerateParmData();
            foreach (EsgynDBParameter parmtmp in ParmCol)
            {
                bool returnB = ParmCol.Contains(parmtmp.ParameterName);
                Console.WriteLine("the parmtmp name is:{0},ParmCol's name is:{1},returnB is :{2}", parmtmp.ParameterName, ParmCol[0].ParameterName,returnB);
                Assert.AreEqual(true, returnB);
            }
            foreach (String name in NegName)
            {
                bool returnB = ParmCol.Contains(name);
                Assert.AreEqual(false, returnB);
            }
        }

        [Test]
        public void Copyto()
        {
            int ParmCount;
            int arraynum = 5;
            int[] index = { 0, 2, 5}; // boundary, normal, exceed 
            GenerateArray(arraynum, "A");

            GenerateParmData();
            for (int i = 0; i < index.Length; i++)
            {
                EsgynDBParameterCollection ParmColTmp = ParmCol;
                ParmCount = ParmColTmp.Count;
                ParmColTmp.CopyTo(Array, index[i]);
                //Assert.AreEqual(ParmCount + arraynum, ParmColTmp.Count);
                Assert.AreEqual(ParmCount + ParmColTmp.Count, Array.Length);
                
                /* NOT SURE WHAT THIS IS... HAVE TO CHECK TEH COPIED ITEMS
                for (int j = 0; j < ParmColTmp.Count; j++)
                {
                    if ((index[i]<j) &&( j<index[i]+arraynum))
                    {  
                         Assert.AreEqual(Array[j-index[i]].ParameterName,ParmColTmp[j].ParameterName);
                         Assert.AreEqual(Array[j-index[i]].Value,ParmColTmp[j].Value);
                         Assert.AreEqual(Array[j-index[i]].DbType,ParmColTmp[j].DbType);
                         Assert.AreEqual(Array[j-index[i]].Direction,ParmColTmp[j].Direction);
                    }
                }
                */
            }
         }

        [Test]
        public void Insert()
        {
            int[] index = new int[3] { 0, 2, 5}; //boundary, normal, exceed
            for (int i = 0; i < index.Length; i++)
            {
                GenerateParmData();
                EsgynDBParameterCollection ParmColTmp = ParmCol;
                EsgynDBParameterCollection ParmColBk = ParmCol;
                int ParmCount = ParmColTmp.Count;
                EsgynDBParameter parmTmp = new EsgynDBParameter("test", "test_ParameterCollection");
                ParmColTmp.Insert(index[i], parmTmp);
                Console.WriteLine("Param count after insert : " + ParmColTmp.Count);
                Assert.AreEqual(ParmCount + 1, ParmColTmp.Count);

                Console.WriteLine("Iteration number : " + i);
                Console.WriteLine("new param should be inserted at position : " + index[i]);
                foreach (EsgynDBParameter np in ParmColTmp)
                {
                    Console.WriteLine("Param name: " + np.ParameterName + " Param value : " + np.Value);
                }

                /*
                for (int j = 0; j < ParmColTmp.Count; j++)
                {
                    if (j < index[i])
                    {
                        Assert.AreEqual(ParmColBk[j].ParameterName, ParmColTmp[j].ParameterName);
                        Assert.AreEqual(ParmColBk[j].Value, ParmColTmp[j].Value);
                        Assert.AreEqual(ParmColBk[j].DbType, ParmColTmp[j].DbType);
                        Assert.AreEqual(ParmColBk[j].Direction, ParmColTmp[j].Direction);
                        Console.WriteLine("index:{0}, j:{1}, ParmColBk and the ParmColTmp are the same", index[i], j);
                    }
                    if (j == index[i])
                    {
                        Assert.AreSame(parmTmp, ParmColTmp[j]);
                        Console.WriteLine("index:{0}, j:{1}, insert parmTmp and the ParmColTmp are the same", index[i], j);
                    }
                    if (j > index[i])
                    {
                        Assert.AreEqual(ParmColBk[j - 1].ParameterName, ParmColTmp[j].ParameterName);
                        Assert.AreEqual(ParmColBk[j - 1].Value, ParmColTmp[j].Value);
                        Assert.AreEqual(ParmColBk[j - 1].DbType, ParmColTmp[j].DbType);
                        Assert.AreEqual(ParmColBk[j - 1].Direction, ParmColTmp[j].Direction);
                        Console.WriteLine("index:{0}, j:{1}, ParmColBk and the ParmColTmp are the same", index[i], j);
                    }
                }
                */
            }
        }
    }
}
