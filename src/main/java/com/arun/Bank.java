package com.arun;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

public class Bank {

	public static void main(String[] args) throws IOException {
		//		Creating and setting Spark Session. It will include spark core+sql+hive
		SparkSession spark = SparkSession.builder().appName("SampleBank").master("local").getOrCreate();		
		spark.sparkContext().setLogLevel("ERROR");		
		Dataset<Row> bankDS = spark.read().format("csv").option("header", true).load("C:\\Arun\\BigData\\Dataset//bank.csv");		
		bankDS.printSchema();		
		bankDS.createOrReplaceTempView("BankDataSet");

		//		Req 1 Fetching Top5 withdrawal
		Dataset<Row> Top5Withdrawal = spark.sql("Select WithdrawalAmt from BankDataSet order by WithdrawalAmt desc limit 5");		
		Top5Withdrawal.show();
		Top5Withdrawal.coalesce(1).write().option("header", true).mode(SaveMode.Overwrite).csv("C:\\Arun\\BigData\\Ouptut\\JavaSpark\\Top5Withdrawal");

		//		Req 2 Fetching Top 5 Cheque Transactions
		Dataset<Row> Top5Cheque = spark.sql("Select ChqNo, WithdrawalAmt from BankDataSet where ChqNo is not null order by WithdrawalAmt desc limit 5");
		Top5Cheque.show();
		Top5Cheque.coalesce(1).write().option("header", true).mode("overwrite").csv("C:\\Arun\\BigData\\Ouptut\\JavaSpark\\Top5Cheque");

		//		Req 3 UDF to categorise		

		HashMap<String, String> categoryMap = new HashMap<String, String>();				
		categoryMap.put("IMPS", "IMPS");
		categoryMap.put("RTGS", "RTGS");
		categoryMap.put("NEFT", "NEFT");
		categoryMap.put("Indiaforensic","India Forensic");
		categoryMap.put("INTERNAL FUND", "Internal Fund Transfer");
		categoryMap.put("CASHDEP", "Cash Deposit");
		categoryMap.put("VISA", "Visa");
		categoryMap.put("RUPAY", "Rupay");
		categoryMap.put("MASTERCARD", "Master Card");
		categoryMap.put("IRCTC", "IRCTC");
		categoryMap.put("Processing Fee", "Processing Fee");
		//		System.out.println(categoryMap.size());
		
		

		spark.sqlContext().udf().register("Categorizer", (String detail) -> {
			
			if(detail!=null) {
				for (String cat : categoryMap.keySet()) {					
					if(detail.contains(cat)) {
//						System.out.println(categoryMap.get(cat));
						return categoryMap.get(cat);
					}
				}
			}
			
//			if(detail!=null && categoryMap.containsKey(detail)) {				
//				return categoryMap.get(detail);
//			}
			
			return "Others";
		}, DataTypes.StringType);
		
		Dataset<Row> TransactionDetail = bankDS.withColumn("Category", org.apache.spark.sql.functions.expr("Categorizer(TransactionDetails)"));
		TransactionDetail.show();
		TransactionDetail.createOrReplaceTempView("BankData");
		Dataset<Row> TransactionSummary = spark.sql("Select Category, max(WithdrawalAmt), max(DepositAmt), min(WithdrawalAmt), min(DepositAmt), "
				+ "avg(WithdrawalAmt), avg(DepositAmt), count(AccountNo) from BankData group by Category");		
		TransactionSummary.show();
		TransactionSummary.coalesce(1).write().option("header", true).mode("overwrite").csv("C:\\Arun\\BigData\\Ouptut\\JavaSpark\\TransactionSummary");
		
		// Req 4 Rank Top 5 Deposit for each Category
		
		Dataset<Row> Top5Cat = TransactionDetail.withColumn("DenseRank", functions.dense_rank().over(Window.partitionBy("Category")
				.orderBy(functions.col("DepositAmt").desc_nulls_last())));
		Top5Cat.createOrReplaceTempView("Top5Summary");		
		Dataset<Row> Top5CatTrans = spark.sql("Select Category, DepositAmt, DenseRank from Top5Summary where DenseRank<=5");
		Top5CatTrans.show();		
		Top5CatTrans.coalesce(1).write().option("header", true).mode("overwrite").csv("C:\\Arun\\BigData\\Ouptut\\JavaSpark\\Top5CatTransaction");
		
		// Req 5 
		Dataset<Row> TransDate = TransactionDetail.withColumn("NewDate", functions.date_format(functions.to_date(
				functions.col("Date"),"dd-MMM-yy"), "MMyy")).withColumn("MonthlyRank", functions.dense_rank().over(Window.partitionBy
						("NewDate","Category").orderBy(functions.col("DepositAmt").desc_nulls_last())));
		TransDate.createOrReplaceTempView("MonthlyTransSummary");
		Dataset<Row> MonthlySummary = spark.sql("Select Category, DepositAmt, MonthlyRank from MonthlyTransSummary where MonthlyRank<=5");
		MonthlySummary.show();
		MonthlySummary.coalesce(1).write().option("header", true).mode("overwrite").csv("C:\\Arun\\BigData\\Ouptut\\JavaSpark\\MonthlySummary");
			
//		spark.stop();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		reader.readLine();
	}

}
