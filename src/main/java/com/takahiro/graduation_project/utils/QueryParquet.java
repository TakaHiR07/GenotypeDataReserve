package com.takahiro.graduation_project.utils;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class QueryParquet {
	private static String APP_NAME = "QueryMetric";
	private static String MASTER = "spark://mu01:7077";
	
	private static final String HADOOP_URI = "hdfs://mu01:9000/";
	
	public static void main(String[] args) throws IOException{
		long boostTime = System.nanoTime();
		Configuration hadoopConf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
		
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		long boostEndTime = System.nanoTime();
		System.out.println("boost time:" + (boostEndTime-boostTime));
		
		String inputFile = args[0];
		String tableName = args[1];
		
		while (true) {
			Scanner in = new Scanner(System.in);
			System.out.print("please input your sql:");
			
			String wholeSql = in.nextLine();
			long startTime = System.nanoTime();
			if (wholeSql.equals("end")) {
				break;
			}
			Dataset<Row> dataset = session.read().parquet(inputFile);
			dataset.createOrReplaceTempView(tableName);
			session.sql(wholeSql).show();
			long endTime = System.nanoTime();
			
			System.out.println("metric:" + (endTime-startTime));
		}
		
		fs.close();
		session.close();
	}
}
