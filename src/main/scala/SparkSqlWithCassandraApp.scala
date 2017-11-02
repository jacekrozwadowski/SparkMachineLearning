import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._

import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.cql.{ ColumnDef, RegularColumn, TableDef, ClusteringColumn, PartitionKeyColumn }
import com.datastax.spark.connector.types._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer._

import java.io.File
import java.util.Currency
import java.util.UUID
import java.util.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat

import com.danielasfregola.randomdatagenerator.RandomDataGenerator._
import org.scalacheck._
import scala.collection.mutable.ListBuffer

object SparkSqlWithCassandraApp {
  
  //Class definition
  case class Customer(id: Int, name: String, county: String) {

    def canEqual(a: Any) = a.isInstanceOf[Customer]
    override def equals(that: Any): Boolean =
      that match {
        case that: Customer => that.canEqual(this) && this.hashCode == that.hashCode
        case _ => false
      }
    override def hashCode: Int = {
      val prime = 31
      var result = 1
      result = prime * result + id;
      return result
    }

  }

  case class Transaction(customerid: Int, year: Int, month: Int, id: String, amount: Int, card: String, status: String)

  case class Balance(customerid: String, card: String, balance: Int, updated_at: Timestamp)
  
  case class CountyStatistics(county: String, year: Int, month: Int, total_amount: Int)
  
  case class Suspicious(customerid: String, card: String, id: String, amount: Int, reported_at: String)
  
  
  //UUID and Timestamp util functions
  def getUUIDTimestamp(uuidString: String): Long = {
    UUID.fromString(uuidString).timestamp();
  }
  
  def getMidnightTimestamp(dateStr: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.parse(dateStr)getTime
  }
  
  //Main part
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("local")
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.testing.memory", "471859200")

    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val session = sqlContext.sparkSession
    
    println("--------Data generation part Start--------")
    println("--------Customer generation--------")
    //Prepare random customer data
    implicit val arbitraryCustomer: Arbitrary[Customer] = Arbitrary {
      for {
        id <- Gen.choose(1, 100)
        firstname <- Gen.oneOf("Daniela", "John", "Martin", "Marco", "Tom", "Kevin", "Jack")
        lastname <- Gen.oneOf("Archer", "Baker", "Brewer", "Butcher",
          "Carter", "Clark", "Cooper", "Cook", "Dyer",
          "Farmer", "Faulkner", "Fisher", "Fuller",
          "Gardener", "Glover", "Head", "Hunt", "Hunter",
          "Judge", "Mason", "Page", "Parker", "Potter",
          "Sawyer", "Slater", "Smith", "Taylor", "Thatcher",
          "Turner", "Weaver", "Woodman", "Wright")
        county <- Gen.oneOf("Cheshire", "Cleveland", "Dorset", "London", "Middlesex", "Norfolk")
      } yield Customer(id, firstname + " " + lastname, county)
    }

    var size = 100
    val customers = random[Customer](size)
    val customerList = customers.sortBy(_.id).distinct
    val customerRDD = sc.parallelize(customerList)
    customerRDD.toDF().show()
    println("No of generated customers: " + customerRDD.collect().length)

    
    
    println("--------Transaction generation--------")
    //Prepare random transaction data
    implicit val arbitraryTransaction: Arbitrary[Transaction] = Arbitrary {
      for {
        year <- Gen.choose(2000, 2017)
        month <- Gen.choose(1, 12)
        amount <- Gen.choose(-200, 3000)
        card_1 <- Gen.choose(1000, 2000)
        card_2 <- Gen.choose(2000, 3000)
        card_3 <- Gen.choose(3000, 4000)
        card_4 <- Gen.choose(4000, 5000)
        status <- Gen.oneOf("PENDING", "COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "REPAID")
      } yield Transaction(0, year, month, UUIDs.timeBased().toString(), amount, card_1 + " " + card_2 + " " + card_3 + " " + card_4, status)
    }

    size = 1000
    val transactionTmp = random[Transaction](size)

    //Mix generated customers with transactions
    val customerListTmp = (1 to 10).map(_ => customerList)
    val customerList10 = customerListTmp.flatMap(x => x)

    val transactionList = transactionTmp.zip(customerList10).map(t => t match {
      case (t, c) => new Transaction(c.id, t.year, t.month, t.id, t.amount, t.card, t.status)
    })

    //Prepare suspicious transactions
    val cheatedClients = customerList.take(3).toList;

    val transactionListEx = transactionList.union(Seq(
      new Transaction(cheatedClients(0).id, 2017, 11, UUIDs.timeBased().toString(), 123, "1565 2509 3791 4667", "COMPLETED"),
      new Transaction(cheatedClients(1).id, 2017, 11, UUIDs.timeBased().toString(), 123, "1033 2444 3221 4861", "COMPLETED"),
      new Transaction(cheatedClients(2).id, 2017, 11, UUIDs.timeBased().toString(), 123, "1416 2498 3103 4822", "COMPLETED"),
      new Transaction(cheatedClients(2).id, 2017, 11, UUIDs.timeBased().toString(), 123, "1416 2498 3103 4822", "COMPLETED")))

    val transactionRDD = sc.parallelize(transactionListEx.toList)
    //transactionRDD.take(10).foreach(println)
    transactionRDD.toDF().show()
    println("No of generated transactions: " + transactionRDD.collect().length)

    
    
    println("--------KEYSPACE creation--------")
    //Prepare keysapce if not exists
    val conn = CassandraConnector(conf)
    conn.withSessionDo { session =>
      session.execute(s"""CREATE KEYSPACE IF NOT EXISTS cc WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    }

    
    
    implicit val rowWriter = SqlRowWriter.Factory
    println("--------Store customers in Cassandra--------")
    try {
      customerRDD.saveAsCassandraTable("cc", "cc_customers", SomeColumns("id", "county", "name"))
    } catch {
      case e: AlreadyExistsException => {
        sc.cassandraTable("cc", "cc_customers").deleteFromCassandra("cc", "cc_customers")
        customerRDD.saveToCassandra("cc", "cc_customers", SomeColumns("id", "county", "name"))
      }
    }
    
    

    println("--------Store Transactions in Cassandra--------")
    try {

      // Define columns
      val customeridCol = new ColumnDef("customerid", PartitionKeyColumn, TextType)
      val yearCol = new ColumnDef("year", PartitionKeyColumn, IntType)
      val monthCol = new ColumnDef("month", PartitionKeyColumn, IntType)
      val idCol = new ColumnDef("id", ClusteringColumn(0), TimeUUIDType)
      val amountCol = new ColumnDef("amount", RegularColumn, IntType)
      val cardCol = new ColumnDef("card", RegularColumn, TextType)
      val statusCol = new ColumnDef("status", RegularColumn, TextType)

      // Create table definition
      val transactionTable = TableDef("cc", "cc_transactions",
                                      Seq(customeridCol, yearCol, monthCol),
                                      Seq(idCol),
                                      Seq(amountCol, cardCol, statusCol))

      // Map rdd into custom data structure and create table
      transactionRDD.saveAsCassandraTableEx(transactionTable)

    } catch {
      case e: AlreadyExistsException => {
        sc.cassandraTable("cc", "cc_transactions").deleteFromCassandra("cc", "cc_transactions")
        transactionRDD.saveToCassandra("cc", "cc_transactions", SomeColumns("customerid", "year", "month", "id", "amount", "card", "status"))
      }
    }
    
    println("--------Data generation part End--------")
    
    
    
    println("--------Data analyze part Start--------")
    
    println("--------Current balance calculation--------")
    val includedStatuses = Set("COMPLETED", "REPAID")
    val d = new Date()
    val now = new Timestamp(d.getTime);
    
    //RDD API
    val ccBalanceRDD = sc.cassandraTable("cc", "cc_transactions")
    .select("customerid", "amount", "card", "status", "id")
    .where("id < minTimeuuid(?)", now)
    .filter(includedStatuses contains _.getString("status"))
    .keyBy(row => (row.getString("customerid"), row.getString("card")))
    .map { case (key, value) => (key, value.getInt("amount")) }
    .reduceByKey(_ + _)
    .map { case ((customerid, card), balance) => new Balance(customerid, card, balance, now) }
    
    ccBalanceRDD.toDF().show()
    
    
    println("--------Store Balance in Cassandra--------")
    // Define columns
    var customeridCol = new ColumnDef("customerid",PartitionKeyColumn,TextType)
    var cardCol = new ColumnDef("card",PartitionKeyColumn,TextType)
    val balanceCol = new ColumnDef("balance",RegularColumn,IntType)
    val updatedAtCol = new ColumnDef("updated_at",RegularColumn,TimestampType)
      
    // Create table definition
    val balanceTable = TableDef("cc","cc_balance",
                                Seq(customeridCol,cardCol),
                                Seq(),
                                Seq(balanceCol, updatedAtCol)
                                )
                                      
    try {
      // Map rdd into balance data structure and create table
      ccBalanceRDD.saveAsCassandraTableEx(balanceTable)
    } catch {
      case e: AlreadyExistsException => {
        sc.cassandraTable("cc", "cc_balance").deleteFromCassandra("cc", "cc_balance")
        ccBalanceRDD.saveToCassandra("cc", "cc_balance", SomeColumns("customerid", "card", "balance", "updated_at"))
      }
    }
    
    
    
    
    println("--------County statistics calculation--------")
    session.read.cassandraFormat("cc_transactions", "cc")
    .load()
    .createOrReplaceTempView("spark_cc_transactions")
    
    session.read.cassandraFormat("cc_customers", "cc")
    .load()
    .createOrReplaceTempView("spark_cc_customers")
    
    
    //DataFrame API
    val countyStatisticsRDD = session.sql(
          """
            |SELECT c.county as county, t.year as year, t.month as month, cast(-SUM(t.amount) as int) as total_amount
            |FROM spark_cc_customers c
            |INNER JOIN spark_cc_transactions as t ON c.id = t.customerid
            |WHERE t.status = 'COMPLETED'
            |GROUP BY c.county, t.year, t.month
          """.stripMargin
        )
    .map(r => new CountyStatistics(r.getAs[String](0), r.getAs[Int](1),  r.getAs[Int](2),  r.getAs[Int](3))).rdd
    
    countyStatisticsRDD.toDF().show()
    
    println("--------Store County statistics in Cassandra--------")
    // Define columns
    val countyCol = new ColumnDef("county",PartitionKeyColumn,TextType)
    val yearCol = new ColumnDef("year",PartitionKeyColumn,IntType)
    val monthCol = new ColumnDef("month",PartitionKeyColumn,IntType)
    val totalAmountCol = new ColumnDef("total_amount",RegularColumn,IntType)
      
    // Create table definition
    val countyStatisticsTable = TableDef("cc","cc_county_statistics",
                                Seq(countyCol,yearCol,monthCol),
                                Seq(),
                                Seq(totalAmountCol)
                                )
    
    try {
      // Map rdd into balance data structure and create table
      countyStatisticsRDD.saveAsCassandraTableEx(countyStatisticsTable)
    } catch {
      case e: AlreadyExistsException => {
        sc.cassandraTable("cc", "cc_county_statistics").deleteFromCassandra("cc", "cc_county_statistics")
          countyStatisticsRDD
          .saveToCassandra("cc", "cc_county_statistics", SomeColumns("county", "year", "month", "total_amount"))
      }
    }
    
    
    println("--------Suspicious Transactions calculation--------")
    val ccTransactions = session
    .read.cassandraFormat("cc_transactions", "cc")
    .load()

    val lostCards = session
    .read.option("header", "true")
    .csv("src/main/resources/lost_cards.csv")
    
    val suspiciousRDD = ccTransactions
    .join(lostCards, "card")
    .filter(row => getUUIDTimestamp(row.getAs("id")) >= getMidnightTimestamp(row.getAs("reported_at")))
    .select("customerid", "card", "id", "amount", "reported_at")
    .map(r => new Suspicious(r.getAs[String](0), r.getAs[String](1), r.getAs[String](2), r.getAs[Int](3), r.getAs[String](4))).rdd
    
    suspiciousRDD.toDF().show()
    
    println("--------Store Suspicious Transactions in Cassandra--------")
    // Define columns
    customeridCol = new ColumnDef("customerid",PartitionKeyColumn,TextType)
    cardCol = new ColumnDef("card",RegularColumn,TextType)
    val idCol = new ColumnDef("id",RegularColumn,TimeUUIDType)
    val amountCol = new ColumnDef("amount",RegularColumn,IntType)
    val reportedAtCol = new ColumnDef("reported_at",RegularColumn,TextType)
      
    // Create table definition
    val suspiciousTable = TableDef("cc","cc_suspicious",
                                Seq(customeridCol),
                                Seq(),
                                Seq(cardCol, idCol, amountCol, reportedAtCol)
                                )
    
      
                                
   try {
      // Map rdd into balance data structure and create table
      suspiciousRDD.saveAsCassandraTableEx(suspiciousTable)
    } catch {
      case e: AlreadyExistsException => {
        sc.cassandraTable("cc", "cc_suspicious").deleteFromCassandra("cc", "cc_suspicious")
          suspiciousRDD
          .saveToCassandra("cc", "cc_suspicious", SomeColumns("customerid", "card", "id", "amount", "reported_at"))
      }
    }
    
    
    println("--------Suspicious Transactions with customer--------")
    session.read.cassandraFormat("cc_suspicious", "cc")
    .load()
    .createOrReplaceTempView("spark_cc_suspicious")
    
    session.read.cassandraFormat("cc_customers", "cc")
    .load()
    .createOrReplaceTempView("spark_cc_customers")
    
    session.sql(
          """
            |SELECT s.customerid as customerid, 
            |       s.card as card, 
            |       s.amount as amount,
            |       c.county as county,
            |       c.name as name
            |FROM spark_cc_suspicious s
            |INNER JOIN spark_cc_customers as c ON s.customerid = c.id
          """.stripMargin
        )
    .show()
    
    
    
    println("--------Data analyze part End--------")
    sc.stop()
  }

}


/*
 
 Cassandra Sql keyspace and tables definition
 
 CREATE KEYSPACE cc WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };
 CREATE TABLE cc.cc_customers (
    id text PRIMARY KEY,
    county text,
    name text
 );

 CREATE TABLE cc.cc_transactions (
    customerid text,
    year int,
    month int,
    id timeuuid,
    amount int,
    card text,
    status text,
    PRIMARY KEY ((customerid, year, month), id)
 );
 
 CREATE TABLE cc.cc_balance (
    customerid text,
    card text,
    balance int,
    updated_at timestamp,
    PRIMARY KEY ((customerid, card))
 );

 CREATE TABLE cc.cc_county_statistics (
    county text,
    year int,
    month int,
    total_amount int,
    PRIMARY KEY ((county, year,month))
 ); 
 
 CREATE TABLE cc.cc_suspicious (
    customerid text,
    card text,
    id timeuuid,
    amount int,
    reported_at text,
    PRIMARY KEY (customerid)
 ); 
  
 */