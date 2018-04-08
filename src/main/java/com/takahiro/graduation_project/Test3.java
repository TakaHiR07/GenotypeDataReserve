package com.takahiro.graduation_project;

import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test3 {
	private static String APP_NAME = "TestParquet";
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
		/*
		session.sql("select count(*) from chr1 where (hg00100='0|0' or hg00100='1|0' or hg00100='5|0') and "
				+ "(hg00245='0|1' or hg00245='1|1' or hg00245='5|0') and (hg00251='0|1' or hg00251='1|1' or hg00252 = '1|1' "
				+ "or hg00251='5|0')").show();*/

		long num = dataset.filter("(HG00100 = '0|0' OR HG00100 = '1|0' OR HG00100 = '5|0') AND (HG00245 = '0|1' OR "
				+ "HG00245 = '1|1' OR HG00245 = '5|0') AND (hg00251 = '0|1' or hg00251 = '1|1' or hg00251 = '5|0')").count();
		long afterTime = System.nanoTime();


		System.out.println("sql exec time:" + (afterTime-beforeTime));
		
		
		long stopTime = System.nanoTime();
		System.out.println("duration:" + (stopTime-boostTime));
		System.out.println("resultSet count:" + num);
		
		session.close();
	}
}
