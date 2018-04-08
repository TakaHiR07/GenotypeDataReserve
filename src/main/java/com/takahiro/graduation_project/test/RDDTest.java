package com.takahiro.graduation_project.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.roaringbitmap.RoaringBitmap;

public class RDDTest {
	private static String APP_NAME = "Generate";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3noheader.vcf";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication3_vcf.txt";
	private static String HADOOP_URI = "hdfs://mu01:9000/";
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(APP_NAME);
		conf.set("spark.serializer",	"org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class[]{RoaringBitmap.class});
		
		//SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
		SparkSession session = SparkSession.builder().config(conf).master("local").getOrCreate();
		String inputFile = "/media/sf_Desktop/毕业设计/new.vcf";
		
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		dataset.foreach(new VoidFunction<String>() {
			private int count = 0;
			public void call(String t) throws Exception {
				if (count++ == 0) {
					System.out.println("first line:" + t);
				} else {
					System.out.println("di " + count + " line:" + t);
				}
			}
		});
		session.close();
	}
}
