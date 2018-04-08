package com.takahiro.graduation_project;

import java.io.IOException;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Test {
	private static String APP_NAME = "TestParquet";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3withheader.parquet";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication3_parquet.txt";
	//private static Logger logger = LoggerFactory.getLogger(TestParquet.class);
	
	public static void main(String[] args) throws IOException{
		String inputFile = args[0];
		String type = args[1];
		
		long boostTime = System.nanoTime();
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		long loadSetBefore = System.nanoTime();
		Dataset<Row> dataset = session.read().parquet(inputFile);
		dataset.createOrReplaceTempView("chr1");
		long loadSetAfter = System.nanoTime();
		System.out.println("load set time:" + (loadSetAfter-loadSetBefore));
		
		
		long beforeTime = System.nanoTime();
		
		if (type.equals("sql")) {
			session.sql("select count(*) from chr1 where (hg00100='0|0' or hg00100='1|0' or hg00100='5|0') and "
					+ "(hg00245='0|1' or hg00245='1|1' or hg00245='5|0') and (hg00251='0|1' or hg00251='1|1' or hg00252 = '1|1' "
					+ "or hg00251='5|0')").show();
		} else if (type.equals("sparksql")) {
			/*
			long num = dataset.filter("hg00100 = '0|0' or hg00100 = '1|0' or hg00100 = '5|0'").filter("hg00245 = '0|1 or "
					+ "hg00245 = '1|1' or hg00245 = '5|0'").filter("hg00251 = '0|1' or hg00251 = '1|1' or hg00251 = '5|0'").count();
			long afterTime = System.nanoTime();
			Column column = new Column("HG00100 = '0|0' OR HG00100 = '1|0' OR HG00100 = '5|0'").and(new Column("HG00245 = '0|1' OR "
					+ "HG00245 = '1|1' OR HG00245 = '5|0'")).and(new Column("HG00251 = '0|1' OR hg00251 = '1|1' OR HG00251 = '5|0'"));
			long num = dataset.filter(column).count();*/
			Column col1 = dataset.col("HG00100").equalTo("0|0");
			Column col2 = dataset.col("HG00100").equalTo("1|0");
			Column col3 = dataset.col("HG00100").equalTo("5|0");
			
			Column col4 = dataset.col("HG00245").equalTo("0|1");
			Column col5 = dataset.col("HG00245").equalTo("1|1");
			Column col6 = dataset.col("HG00245").equalTo("5|0");
			
			Column col7 = dataset.col("HG00251").equalTo("0|1");
			Column col8 = dataset.col("HG00251").equalTo("1|1");
			Column col9 = dataset.col("HG00251").equalTo("5|0");
			
			Column column = col1.or(col2).or(col3);
			Column column2 = col4.or(col5).or(col6);
			Column column3 = col7.or(col8).or(col9);
			
			long num = dataset.filter(column.and(column2).and(column3)).count();
		}
		long afterTime = System.nanoTime();
		System.out.println("sql exec time:" + (afterTime-beforeTime));
		
		
		long stopTime = System.nanoTime();
		System.out.println("duration:" + (stopTime-boostTime));
		//System.out.println("resultSet count:" + num);
		
		session.close();
	}
}
