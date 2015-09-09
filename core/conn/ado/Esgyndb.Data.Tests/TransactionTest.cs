using System;
using System.Collections.Generic;
using System.Text;

using NUnit.Framework;
using Esgyndb.Data;

namespace TrafAdoTest
{
    [TestFixture]
    public class TransactionTest
    {
        //TestObject testInfo = AdoTestSetup.testInfo;
        TestObject testInfo = new TestObject("ASCII");
        private EsgyndbCommand myCommand;

        [SetUp]
        public void Setup()
        {
            testInfo.connection.Open();
            myCommand = testInfo.connection.CreateCommand();
        }

        [TearDown]
        public void TearDown()
        {
            myCommand.Dispose();
            testInfo.connection.Close();
        }

        [Test]
        public void Commit()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction();
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch(Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            myTrans.Dispose();
        }

        [Test]
        public void Rollback()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction();
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Rollback();

                Console.WriteLine("Both records written and rolled back.");
            }
            catch (Exception e)
            {
                Console.WriteLine("An exception of type " + e.GetType() + " was encountered while attempting to roll back the transaction.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
            }
            myTrans.Dispose();
        }

        [Test]
        public void IsolationLevelReadCommitted()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.ReadCommitted);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Rollback();

                Console.WriteLine("Both records written and rolled back.");
            }
            catch (Exception e)
            {
                Console.WriteLine("An exception of type " + e.GetType() + " was encountered while attempting to roll back the transaction.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.ReadCommitted);
            myTrans.Dispose();
        }

        [Test]
        public void IsloationLevelReadUncommitted()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(2, rowsAffected);
            Console.WriteLine("Both records are written to database.");

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.ReadUncommitted);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "select * from TEMPTAB";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(-1, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.ReadUncommitted);
            myTrans.Dispose();
        }

        [Test]
        //[ExpectedException(typeof(NotSupportedException))]
        [ExpectedException(typeof(ArgumentException))]
        public void IsloationLevelChaos()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.Chaos);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.Chaos);
            myTrans.Dispose();
        }

        [Test]
        public void IsloationLevelUnspecified()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.Unspecified);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.Unspecified);
            myTrans.Dispose();
        }
        [Test]
        public void IsloationLevelSerializable()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.Serializable);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.Serializable);
            myTrans.Dispose();
        }

        [Test]
        public void IsloationLevelRepeatableRead()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.RepeatableRead);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.RepeatableRead);
            myTrans.Dispose();
        }

        [Test]
        //[ExpectedException(typeof(NotSupportedException))]
        [ExpectedException(typeof(ArgumentException))]
        public void IsloationLevelSnapshot()
        {
            int rowsAffected;
            try
            {
                myCommand.CommandText = "drop table TEMPTAB cascade";
                myCommand.ExecuteNonQuery();
            }
            catch { }

            myCommand.CommandText = "create table TEMPTAB (C int) no partition";
            rowsAffected = myCommand.ExecuteNonQuery();
            Assert.AreEqual(-1, rowsAffected);

            // Start a local transaction.
            EsgyndbTransaction myTrans = testInfo.connection.BeginTransaction(System.Data.IsolationLevel.Snapshot);
            // Enlist the command in the current transaction.
            myCommand.Transaction = myTrans;
            try
            {
                myCommand.CommandText = "insert into TEMPTAB values(1),(2)";
                rowsAffected = myCommand.ExecuteNonQuery();
                Assert.AreEqual(2, rowsAffected);

                myTrans.Commit();

                //myCommand.CommandText = "select * from TEMPTAB";
                //rowsAffected = myCommand.ExecuteNonQuery();
                //Assert.AreEqual(-1, rowsAffected);

                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception e)
            {
                try
                {
                    myTrans.Rollback();
                }
                catch (EsgyndbException ex)
                {
                    if (myTrans.Connection != null)
                    {
                        Console.WriteLine("An exception of type " + ex.GetType() + " was encountered while attempting to roll back the transaction.");
                        Console.WriteLine("Exception.Message: {0}", ex.Message);
                    }
                }
                Console.WriteLine("An exception of type " + e.GetType() + "was encountered while inserting the data.");
                Console.WriteLine("Exception.Message: {0}", e.Message);
                Console.WriteLine("Neither record was written to database.");
            }
            Assert.AreEqual(myTrans.IsolationLevel, System.Data.IsolationLevel.Snapshot);
            myTrans.Dispose();
        }
    }
}
