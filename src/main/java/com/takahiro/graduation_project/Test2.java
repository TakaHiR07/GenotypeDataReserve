package com.takahiro.graduation_project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test2 {
	private static String APP_NAME = "TestParquetWithReplication3";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3withheader.parquet";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication3_parquet.txt";
	//private static Logger logger = LoggerFactory.getLogger(TestParquet.class);
	
	public static void main(String[] args) throws IOException{
		long boostTime = System.nanoTime();
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		long loadSetBefore = System.nanoTime();
		Dataset<Row> dataset = session.read().parquet(DATASET_PATH);
		long loadSetAfter = System.nanoTime();
		System.out.println("load set time:" + (loadSetAfter-loadSetBefore));
		
		
		long beforeTime = System.nanoTime();
		String str1 = "0|0", str2 = "1|0", str3 = "5|0";
		long num = dataset.filter("HG00100 = '" + str1 + "' OR HG00100 = '" + str2 + "' OR HG00100 = '" + str3 + "'").count();
		long afterTime = System.nanoTime();
		System.out.println("sql exec time:" + (afterTime-beforeTime));
		System.out.println("count:" + num);
		
		
		long stopTime = System.nanoTime();
		System.out.println("duration:" + (stopTime-boostTime));
		
		session.close();
	}
}
