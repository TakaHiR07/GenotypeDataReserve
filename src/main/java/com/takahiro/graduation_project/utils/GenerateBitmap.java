package com.takahiro.graduation_project.utils;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.roaringbitmap.RoaringBitmap;

import com.clearspring.analytics.util.Bits;

public class GenerateBitmap {
	private static String APP_NAME = "Generate";
	private static String MASTER = "spark://mu01:7077";
	private static String DATASET_PATH = "hdfs://mu01:9000/user/fanjy/chr1_split3noheader.vcf";
	private static String FILE_PATH = "hdfs://mu01:9000/user/fanjy/result_chr1_replication3_vcf.txt";
	private static String HADOOP_URI = "hdfs://mu01:9000/";
	
	//输入的文件必须只有所有的基因型数据
	public static void generateRoaringBitmap(String inputFile, String outputFile, String headerFile, SparkSession session) throws IOException {
		SparkConf conf = new SparkConf().setAppName(APP_NAME);
		conf.set("spark.serializer",	"org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(new Class[]{RoaringBitmap.class});

		
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		JavaRDD<String> headerRDD = session.read().textFile(headerFile).javaRDD();
		//感觉应该不会线程不安全
		List<String> headerList = headerRDD.collect();
		List<String> header = new ArrayList<String>();
		String[] headers = headerList.get(0).split("\t");
		for (int i = 0; i < headers.length; i++) {
			header.add(headers[i]);
		}
		System.out.println("header.size():" + header.size());
		
		//进行生成位图操作
		final RoaringBitmap[] roaringBitmaps = new RoaringBitmap[header.size()*4];
		for (int i = 0; i < header.size()*4; i++) {
			roaringBitmaps[i] = new RoaringBitmap();
		}
		List<String> data = dataset.collect();
		for (int i = 0; i < data.size(); i++) {
			String[] groups = data.get(i).split("\t");
			for (int j = 0; j < groups.length; j++) {
				String genotype = groups[j];
				if (genotype.equals("0|0")) {
					roaringBitmaps[j*4+0].add(i);;
				} else if (genotype.equals("0|1")) {
					roaringBitmaps[j*4+1].add(i);
				} else if (genotype.equals("1|0")) {
					roaringBitmaps[j*4+2].add(i);
				} else if (genotype.equals("1|1")) {
					roaringBitmaps[j*4+3].add(i);
				}
			}
		}
		
		
		//将RoaringBitmap序列化到hdfs中
		for (int i = 0; i < roaringBitmaps.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(roaringBitmaps[i]);
			oos.close();
		}
	}
	
	public static void generateBitmap(String inputFile, String outputFile, String headerFile, SparkSession session) throws IOException{
		
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		JavaRDD<String> headerRDD = session.read().textFile(headerFile).javaRDD();
		//感觉应该不会线程不安全
		List<String> headerList = headerRDD.collect();
		List<String> header = new ArrayList<String>();
		String[] headers = headerList.get(0).split("\t");
		for (int i = 0; i < headers.length; i++) {
			header.add(headers[i]);
		}
		System.out.println(header.size());
		
		//进行生成位图操作
		final BitSet[] bitSets = new BitSet[header.size()*4];
		for (int i = 0; i < header.size()*4; i++) {
			bitSets[i] = new BitSet(32);
		}
		List<String> data = dataset.collect();
		for (int i = 0; i < data.size(); i++) {
			String[] groups = data.get(i).split("\t");
			for (int j = 0; j < groups.length; j++) {
				String genotype = groups[j];
				if (genotype.equals("0|0")) {
					bitSets[j*4+0].set(i);
				} else if (genotype.equals("0|1")) {
					bitSets[j*4+1].set(i);
				} else if (genotype.equals("1|0")) {
					bitSets[j*4+2].set(i);
				} else if (genotype.equals("1|1")) {
					bitSets[j*4+3].set(i);
				}
			}
		}
		
		
		//将bitmap序列化到hdfs中
		for (int i = 0; i < bitSets.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(bitSets[i]);
			oos.close();
		}
	}
	
	//生成Bitset，输入的文件包含header,header以小写为准
	public static void generateBitmapWithHeader(String inputFile, String outputFile, SparkSession session) throws IOException{
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		//感觉应该不会线程不安全
		List<String> header = new ArrayList<String>();
		
		//进行生成位图操作
		BitSet[] bitSets = null;
		List<String> data = dataset.collect();
		for (int i = 0; i < data.size(); i++) {
			if (i == 0) {
				String[] headers = data.get(i).split("\t");
				for (int j = 0; j < headers.length; j++) {
					header.add(headers[j].toLowerCase());
				}
				System.out.println("header.size():" + header.size());
				bitSets = new BitSet[header.size()*4];
				for (int j = 0; j < header.size()*4; j++) {
					bitSets[j] = new BitSet(32);
				}
			} else {
				String[] groups = data.get(i).split("\t");
				for (int j = 0; j < groups.length; j++) {
					String genotype = groups[j];
					if (genotype.equals("0|0")) {
						bitSets[j*4+0].set(i);
					} else if (genotype.equals("0|1")) {
						bitSets[j*4+1].set(i);
					} else if (genotype.equals("1|0")) {
						bitSets[j*4+2].set(i);
					} else if (genotype.equals("1|1")) {
						bitSets[j*4+3].set(i);
					}
				}
			}
		}
		
		
		//将bitmap序列化到hdfs中
		for (int i = 0; i < bitSets.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(bitSets[i]);
			oos.close();
		}
	}
	
	//生成roaringbitmap，输入的文件包含header
	public static void generateRoaringBitmapWithHeader(String inputFile, String outputFile, SparkSession session) throws IOException{
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		List<String> header = new ArrayList<String>();
		
		//进行生成位图操作
		RoaringBitmap[] roaringBitmaps = null;
		List<String> data = dataset.collect();
		for (int i = 0; i < data.size(); i++) {
			if (i == 0) {
				String[] headers = data.get(i).split("\t");
				for (int j = 0; j < headers.length; j++) {
					header.add(headers[j].toLowerCase());
				}
				System.out.println("header.size():" + header.size());
				roaringBitmaps = new RoaringBitmap[header.size()*4];
				for (int j = 0; j < header.size()*4; j++) {
					roaringBitmaps[j] = new RoaringBitmap();
				}
			} else {
				String[] groups = data.get(i).split("\t");
				for (int j = 0; j < groups.length; j++) {
					String genotype = groups[j];
					if (genotype.equals("0|0")) {
						roaringBitmaps[j*4+0].add(i);;
					} else if (genotype.equals("0|1")) {
						roaringBitmaps[j*4+1].add(i);
					} else if (genotype.equals("1|0")) {
						roaringBitmaps[j*4+2].add(i);
					} else if (genotype.equals("1|1")) {
						roaringBitmaps[j*4+3].add(i);
					}
				}
			}
		}
		
			
		//将RoaringBitmap序列化到hdfs中
		for (int i = 0; i < roaringBitmaps.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(roaringBitmaps[i]);
			oos.close();
		}
	}
	
	//生成Bitset，输入的文件不包含header,不包含collect操作，而是遍历RDD
	public static void generateBitmap2(String inputFile, String outputFile, String headerFile, SparkSession session) throws IOException {
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		JavaRDD<String> headerRDD = session.read().textFile(headerFile).javaRDD();
		//感觉应该不会线程不安全
		List<String> headerList = headerRDD.collect();
		final List<String> header = new ArrayList<String>();
		String[] headers = headerList.get(0).split("\t");
		for (int i = 0; i < headers.length; i++) {
			header.add(headers[i]);
		}
		System.out.println("header.size():" + header.size());
		
		//进行生成位图操作
		final BitSet[] bitSets = new BitSet[header.size()*4];
		for (int i = 0; i < header.size()*4; i++) {
			bitSets[i] = new BitSet();
		}
		dataset.foreach(new VoidFunction<String>() {
			private int i = 0;
			public void call(String data) throws Exception {
				String[] groups = data.split("\t");
				for (int j = 0; j < groups.length; j++) {
					String genotype = groups[j];
					if (genotype.equals("0|0")) {
						bitSets[j*4+0].set(i++);;
					} else if (genotype.equals("0|1")) {
						bitSets[j*4+1].set(i++);
					} else if (genotype.equals("1|0")) {
						bitSets[j*4+2].set(i++);
					} else if (genotype.equals("1|1")) {
						bitSets[j*4+3].set(i++);
					}
				}
			}
		});
		
		//将RoaringBitmap序列化到hdfs中
		for (int i = 0; i < bitSets.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(bitSets[i]);
			oos.close();
		}
	}
	
	//生成roaringbitmap，输入的文件不包含header,不包含collect操作，而是遍历RDD
	public static void generateRoaringBitmap2(String inputFile, String outputFile, String headerFile, SparkSession session) throws IOException{		
		JavaRDD<String> dataset = session.read().textFile(inputFile).javaRDD();
		JavaRDD<String> headerRDD = session.read().textFile(headerFile).javaRDD();
		//感觉应该不会线程不安全
		List<String> headerList = headerRDD.collect();
		final List<String> header = new ArrayList<String>();
		String[] headers = headerList.get(0).split("\t");
		for (int i = 0; i < headers.length; i++) {
			header.add(headers[i]);
		}
		System.out.println("header.size():" + header.size());
		
		//进行生成位图操作
		final RoaringBitmap[] roaringBitmaps = new RoaringBitmap[header.size()*4];
		for (int i = 0; i < header.size()*4; i++) {
			roaringBitmaps[i] = new RoaringBitmap();
		}
		dataset.foreach(new VoidFunction<String>() {
			private int i = 0;
			public void call(String data) throws Exception {
				String[] groups = data.split("\t");
				for (int j = 0; j < groups.length; j++) {
					String genotype = groups[j];
					if (genotype.equals("0|0")) {
						roaringBitmaps[j*4+0].add(i++);;
					} else if (genotype.equals("0|1")) {
						roaringBitmaps[j*4+1].add(i++);
					} else if (genotype.equals("1|0")) {
						roaringBitmaps[j*4+2].add(i++);
					} else if (genotype.equals("1|1")) {
						roaringBitmaps[j*4+3].add(i++);
					}
				}
			}
		});
		
		//将RoaringBitmap序列化到hdfs中
		for (int i = 0; i < roaringBitmaps.length; i++) {
			String headerName = header.get(i/4);
			String genotype = "";
			if (i%4 == 0) {
				genotype = "0|0";
			} else if (i%4 == 1) {
				genotype = "0|1";
			} else if (i%4 == 2) {
				genotype = "1|0";
			} else if (i%4 == 3) {
				genotype = "1|1";
			}
			String filename = outputFile + headerName + "_" + genotype;
			Configuration hadoopConf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
			Path path = new Path(filename);
			ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
			oos.writeObject(roaringBitmaps[i]);
			oos.close();
		}
	}
	
	//生成位图索引的metadata文件
	public static void generateMetadata(String outputFile) {
		
	}
	
	public static void main(String[] args) throws IOException {
		String type = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		
		SparkConf conf = new SparkConf().setAppName(APP_NAME);
		conf.set("spark.serializer",	"org.apache.spark.serializer.KryoSerializer");
		
		if (type.equals("roaringbitmap")) {
			String headerFile = args[3];
			conf.registerKryoClasses(new Class[]{RoaringBitmap.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateRoaringBitmap(inputFile, outputFile, headerFile, session);
		} else if (type.equals("bitset")) {
			String headerFile = args[3];
			conf.registerKryoClasses(new Class[]{BitSet.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateBitmap(inputFile, outputFile, headerFile, session);
		} else if (type.equals("roaringbitmapWithHeader")) {
			conf.registerKryoClasses(new Class[]{RoaringBitmap.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateRoaringBitmapWithHeader(inputFile, outputFile, session);
		} else if (type.equals("generateBitmapWithHeader")) {
			conf.registerKryoClasses(new Class[]{BitSet.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateBitmapWithHeader(inputFile, outputFile, session);
		} else if (type.equals("bitset2")) {
			String headerFile = args[3];
			conf.registerKryoClasses(new Class[]{BitSet.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateBitmap2(inputFile, outputFile, headerFile, session);
		} else if (type.equals("roaringbitmap2")) {
			String headerFile = args[3];
			conf.registerKryoClasses(new Class[]{RoaringBitmap.class});
			SparkSession session = SparkSession.builder().config(conf).master(MASTER).getOrCreate();
			generateRoaringBitmap2(inputFile, outputFile, headerFile, session);
		}
	}
}
