package com.takahiro.graduation_project.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LoadDatasetTest {
	private static String APP_NAME = "Generate";
	private static String MASTER = "spark://mu01:7077";
	
	public static void main(String[] args) {
		long startTime = System.nanoTime();
		
		String inputFile = args[0];
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		Dataset<Row> dataset = session.read().parquet(inputFile);
		dataset.cache();
		
		long endTime = System.nanoTime();
		System.out.println(endTime-startTime);
		
	}
}
