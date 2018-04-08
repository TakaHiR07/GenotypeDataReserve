package com.takahiro.graduation_project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestJsonWithReplication5 {
	private static String APP_NAME = "TestJsonWithReplication5";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3withheader.json";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication5_json.txt";
	//private static Logger logger = LoggerFactory.getLogger(TestParquet.class);
	
	public static void main(String[] args) throws IOException{
		long boostTime = System.nanoTime();
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		long loadSetBefore = System.nanoTime();
		Dataset<Row> dataset = session.read().json(DATASET_PATH);
		long loadSetAfter = System.nanoTime();
		System.out.println("load set time:" + (loadSetAfter-loadSetBefore));
		
		long datasetCount =dataset.count();
		System.out.println("dataset count:" + datasetCount);
		
		long beforeTime = System.nanoTime();
		Dataset<Row> resultSet = dataset.select("POS").filter("POS >= " + 10000 + " AND POS <= " + 100000);
		long afterTime = System.nanoTime();
		System.out.println("sql exec time:" + (afterTime-beforeTime));
		
		long resultSetCount = resultSet.count();
		System.out.println("query row number:" + resultSetCount);
		
		long stopTime = System.nanoTime();
		System.out.println("duration:" + (stopTime-boostTime));
		
		List<String> list = new ArrayList<String>();
		list.add("query row number:" + resultSetCount);
		list.add("dataset count:" + datasetCount);
		list.add("load set time:" + (loadSetAfter-loadSetBefore));
		list.add("sql exec time:"+String.valueOf(afterTime-beforeTime));
		list.add("duration:"+String.valueOf(stopTime-boostTime));
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		JavaRDD<String> javaRDD = sc.parallelize(list);
		javaRDD.repartition(1).saveAsTextFile(FILE_PATH);
		
		sc.close();
		session.close();
	}
}
