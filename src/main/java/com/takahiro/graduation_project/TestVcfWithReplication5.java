package com.takahiro.graduation_project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class TestVcfWithReplication5 {
	private static String APP_NAME = "TestVcfWithReplication5";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3noheader.vcf";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication5_vcf.txt";
	//private static Logger logger = LoggerFactory.getLogger(TestParquet.class);
	
	public static void main(String[] args) throws IOException{
		long boostTime = System.nanoTime();
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		
		long loadSetBefore = System.nanoTime();
		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());
		JavaRDD<String> dataset = sc.textFile(DATASET_PATH);
		long loadSetAfter = System.nanoTime();
		System.out.println("load set time:" + (loadSetAfter-loadSetBefore));
		
		long datasetCount = dataset.count();
		System.out.println("dataset count:" + datasetCount);
		
		long beforeTime = System.nanoTime();
		JavaRDD<String[]> resultSet = dataset.map(new Function<String, String[]>() {
			public String[] call(String line) throws Exception {
				return line.split("\t");
			}
		}).filter(new Function<String[], Boolean>() {
			public Boolean call(String[] line) throws Exception {
				if (Long.valueOf(line[0]) >= 10000 && Long.valueOf(line[0]) <= 100000) {
					return true;
				} else {
					return false;
				}
			}
		});


		long resultSetCount = resultSet.count();
		System.out.println("query row number:" + resultSetCount);
		
		long afterTime = System.nanoTime();
		System.out.println("sql exec time:" + (afterTime-beforeTime));

		
		long stopTime = System.nanoTime();
		System.out.println("duration:" + (stopTime-boostTime));
		
		List<String> list = new ArrayList<String>();
		list.add("query row number:" + resultSetCount);
		list.add("dataset count:" + datasetCount);
		list.add("load set time:" + (loadSetAfter-loadSetBefore));
		list.add("sql exec time:"+String.valueOf(afterTime-beforeTime));
		list.add("duration:"+String.valueOf(stopTime-boostTime));
		JavaRDD<String> javaRDD = sc.parallelize(list);
		javaRDD.repartition(1).saveAsTextFile(FILE_PATH);
		
		sc.close();
		session.close();
	}
}
