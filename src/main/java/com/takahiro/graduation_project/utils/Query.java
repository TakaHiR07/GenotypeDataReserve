package com.takahiro.graduation_project.utils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.roaringbitmap.RoaringBitmap;

import com.takahiro.graduation_project.utils.Parser.BinaryTree;
import com.takahiro.graduation_project.utils.Parser.State;

public class Query {
	private static String APP_NAME = "QueryMetric";
	private static String MASTER = "spark://mu01:7077";
	
	private static final String HADOOP_URI = "hdfs://mu01:9000/";
	
	private static final String equal_pattern = "[a-zA-Z0-9]*=[\\'\\|a-zA-Z0-9]*";
	private static final String notequal_pattern = "[a-zA-Z0-9]*!=[\\'\\|a-zA-Z0-9]*";
	private static final String in_pattern = "[a-zA-Z0-9]*\\sin\\s\\([\\'\\|\\,a-zA-Z0-9]*\\)";
	private static final String quotemark_pattern = "(?<=\\')(.+?)(?=\\')";
	private static final String bracket_pattern = "(?<=\\()(.+?)(?=\\))";
	
	private static final Pattern QUOTE_PATTERN = Pattern.compile(quotemark_pattern);
	private static final Pattern BRACKET_PATTERN = Pattern.compile(bracket_pattern);
	
	
	private static String[] operators = {"=", "<", ">", "<=", ">=", "!=", "(", ")", " "};

	
	public static void main(String[] args) throws IOException,ClassNotFoundException{
		long boostTime = System.nanoTime();
		Configuration hadoopConf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
		
		SparkSession session = SparkSession.builder().appName(APP_NAME).master(MASTER).getOrCreate();
		
		long boostEndTime = System.nanoTime();
		System.out.println("boost time:" + (boostEndTime-boostTime));
		
		String type = args[0];
		String inputFile = args[1];
		
		while (true) {
			Scanner in = new Scanner(System.in);
			System.out.print("please input your sql:");
			
			String wholeSql = in.nextLine();
			long startTime = System.nanoTime();
			if (wholeSql.equals("end")) {
				break;
			}
			//将select xxx from xxx where xxx的形式切分
			String[] groups = wholeSql.split("where");
			String sql = groups[1].trim();
			String front = groups[0].trim();
			String[] ss = front.split("from");
			String selectType = ss[0].trim().split("select")[1].trim();
			String tableName = ss[1].trim();
			
			query(type, selectType, sql, wholeSql, tableName, inputFile, fs, session);
			
			long endTime = System.nanoTime();
			System.out.println("metric:" + (endTime-startTime));
		}
		
		fs.close();
		session.close();
	

	}
	
	public static void query(String type, String returnType, String sql, String wholeSql, String tableName, String inputFile, 
			FileSystem fs, SparkSession session) throws IllegalArgumentException, ClassNotFoundException, IOException {
		Parser parser = new Parser();
		BinaryTree root = parser.parse(sql, State.NORMAL, null);
		if (judge(root)) {
			if (type.equals("bitset")) {
				Stack<BitSet> stack = new Stack<BitSet>();
				postOrderTraversal(root, stack, inputFile, fs);
				BitSet bitSet = stack.firstElement();
				if (returnType.equals("count(*)")) {
					System.out.println("result is : " + bitSet.cardinality());
				} else if (returnType.equals("pct()")) {
					
				}
			} else if (type.equals("roaringbitmap")) {
				Stack<RoaringBitmap> stack = new Stack<RoaringBitmap>();
				postOrderTraversal2(root, stack, inputFile, fs);
				RoaringBitmap roaringBitmap = stack.firstElement();
				if (returnType.equals("count(*)")) {
					System.out.println("result is : " + roaringBitmap.getCardinality());
				}
			}
		} else {		//如果查询的内容不是纯基因型,转向查询parquet
			Dataset<Row> dataset = session.read().parquet(inputFile);
			dataset.createOrReplaceTempView(tableName);
			session.sql(wholeSql).show();
		}
		
	}
	
	//判断查询的列是否是纯基因型，如果是的话返回true，否则返回false
	public static boolean judge(BinaryTree root) throws IOException{
		HashSet<String> header = new HashSet<String>();
		header.add("pos");
		header.add("id");
		header.add("ref");
		header.add("alt");
		header.add("qual");
		header.add("filter");
		header.add("info");
		header.add("format");
		return searchChild(root, header);
	}
	private static boolean searchChild(BinaryTree root, HashSet<String> header) {
		boolean res = true;
		if (root != null) {
			if (root.left == null && root.right == null) {	//说明这是一个叶子节点
				String data = root.data.toString();
				data = data.split(" ")[0];
				for (int i = 0; i < operators.length; i++) {
					if (data.contains(operators[i])) {
						data = data.split(operators[i])[0];
						data = data.toLowerCase();		//获取小写的列名称
						if (header.contains(data)) {
							res = false;
							return res;
						}
					}
				}
				if (header.contains(data)) {
					res = false;
					return res;
				}
			}
			res = res && searchChild(root.left, header);
			res = res && searchChild(root.right, header);
		}
		return res;
	}
	
	//可能还要再加一个判断查询是否是count(*),pct(*)类型????
	
	
	//从BitSet位图中执行查询
	public static void postOrderTraversal(BinaryTree root, Stack<BitSet> stack, String inputFile, FileSystem fs) throws IllegalArgumentException, ClassNotFoundException{
		if (root != null) {
			postOrderTraversal(root.left,stack, inputFile, fs);
			postOrderTraversal(root.right,stack, inputFile, fs);
			
			if (root.data != null) {
				String value = root.data.toString(); 
				if (value.equals("and")) {
					BitSet bitSet2 = stack.pop();
					BitSet bitSet1 = stack.pop();
					if (bitSet1 == null || bitSet2 == null) {		//处理当查询的字段不在位图索引的情况，比如基因型为5|0，压根没有这个基因型
						stack.push(null);
					} else {
						bitSet1.and(bitSet2);
						stack.push(bitSet1);
					}
				} else if (value.equals("or")) {
					BitSet bitSet2 = stack.pop();
					BitSet bitSet1 = stack.pop();
					if (bitSet1 == null && bitSet2 == null) {
						stack.push(null);
					} else if (bitSet1 == null) {
						stack.push(bitSet2);
					} else if (bitSet2 == null) {
						stack.push(bitSet1);
					} else {
						bitSet1.or(bitSet2);
						stack.push(bitSet1);
					}
				} else if (value.matches(equal_pattern) ){
					String[] group = value.split("=");
					if (group.length == 2) {
						String key = group[0], val = group[1];
						if (val.contains("'")) {
							Matcher matcher = QUOTE_PATTERN.matcher(val);
							while(matcher.find()) {
								val = matcher.group();
							}
						}
						String bitmapFile = inputFile + "/" + key.toLowerCase() + "_" + val;
						try {
							ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(bitmapFile))));
							BitSet bitset = (BitSet) ins.readObject();
							stack.push(bitset);
						} catch (IOException e) {		//处理当查询的字段不在位图索引的情况，比如基因型为5|0，压根没有这个基因型
							BitSet bitSet = null;
							stack.push(null);
						}
					} else {
						throw new ArrayIndexOutOfBoundsException("value.split(=)");
					}
				} else if (value.matches(notequal_pattern)) {
					
				} else if (value.matches(in_pattern)) {
					String[] group = value.split(" ");
					if (group.length == 3) {
						String key = group[0], op = group[1], val = group[2];
						Matcher matcher = BRACKET_PATTERN.matcher(val);
						while(matcher.find()) {
							val = matcher.group();
						}
						String[] eachVals = val.split(",");
						
						boolean flag = false;
						BitSet bitset1 = null, bitset2 = null;
						for (String eachVal : eachVals) {
							Matcher matcher2 = QUOTE_PATTERN.matcher(eachVal.trim());
							while (matcher2.find()) {
								eachVal = matcher2.group();
							}
							String bitmapFile = inputFile + "/" + key.toLowerCase() + "_" + eachVal;
							try {
								ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(bitmapFile))));
								if (flag == false) {
									bitset1 = (BitSet) ins.readObject();
									flag = true;
								} else {
									bitset2 = (BitSet) ins.readObject();
									bitset1.or(bitset2);
								}
							} catch (IOException e) {
								if (flag == false) {
									bitset1 = new BitSet();
									flag = true;
								} else {
									
								}
							}
						}
						stack.push(bitset1);
					} else {
						throw new ArrayIndexOutOfBoundsException("value.split( )");
					}
				} 
				//后面再处理其他的类型
			}
		}
	}
	
	//从RoaringBitmap位图中执行查询
	public static void postOrderTraversal2(BinaryTree root, Stack<RoaringBitmap> stack, String inputFile, FileSystem fs) throws IllegalArgumentException, ClassNotFoundException{
		if (root != null) {
			postOrderTraversal2(root.left,stack, inputFile, fs);
			postOrderTraversal2(root.right,stack, inputFile, fs);
			
			if (root.data != null) {
				String value = root.data.toString(); 
				if (value.equals("and")) {
					RoaringBitmap bitSet2 = stack.pop();
					RoaringBitmap bitSet1 = stack.pop();
					if (bitSet1 == null || bitSet2 == null) {		//处理当查询的字段不在位图索引的情况，比如基因型为5|0，压根没有这个基因型
						stack.push(null);
					} else {
						bitSet1.and(bitSet2);
						stack.push(bitSet1);
					}
				} else if (value.equals("or")) {
					RoaringBitmap bitSet2 = stack.pop();
					RoaringBitmap bitSet1 = stack.pop();
					if (bitSet1 == null && bitSet2 == null) {
						stack.push(null);
					} else if (bitSet1 == null) {
						stack.push(bitSet2);
					} else if (bitSet2 == null) {
						stack.push(bitSet1);
					} else {
						bitSet1.or(bitSet2);
						stack.push(bitSet1);
					}
				} else if (value.matches(equal_pattern) ){
					String[] group = value.split("=");
					if (group.length == 2) {
						String key = group[0], val = group[1];
						if (val.contains("'")) {
							Matcher matcher = QUOTE_PATTERN.matcher(val);
							while(matcher.find()) {
								val = matcher.group();
							}
						}
						String bitmapFile = inputFile + "/" + key.toLowerCase() + "_" + val;
						try {
							ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(bitmapFile))));
							RoaringBitmap bitset = (RoaringBitmap) ins.readObject();
							stack.push(bitset);
						} catch (IOException e) {		//处理当查询的字段不在位图索引的情况，比如基因型为5|0，压根没有这个基因型
							stack.push(null);
						}
					} else {
						throw new ArrayIndexOutOfBoundsException("value.split(=)");
					}
				} else if (value.matches(notequal_pattern)) {
					
				} else if (value.matches(in_pattern)) {
					String[] group = value.split(" ");
					if (group.length == 3) {
						String key = group[0], op = group[1], val = group[2];
						Matcher matcher = BRACKET_PATTERN.matcher(val);
						while(matcher.find()) {
							val = matcher.group();
						}
						String[] eachVals = val.split(",");
						
						boolean flag = false;
						RoaringBitmap bitset1 = null, bitset2 = null;
						for (String eachVal : eachVals) {
							Matcher matcher2 = QUOTE_PATTERN.matcher(eachVal.trim());
							while (matcher2.find()) {
								eachVal = matcher2.group();
							}
							String bitmapFile = inputFile + "/" + key.toLowerCase() + "_" + eachVal;
							try {
								ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(bitmapFile))));
								if (flag == false) {
									bitset1 = (RoaringBitmap) ins.readObject();
									flag = true;
								} else {
									bitset2 = (RoaringBitmap) ins.readObject();
									bitset1.or(bitset2);
								}
							} catch (IOException e) {
								if (flag == false) {
									bitset1 = new RoaringBitmap();
									flag = true;
								} else {
									
								}
							}
						}
						stack.push(bitset1);
					} else {
						throw new ArrayIndexOutOfBoundsException("value.split( )");
					}
				} 
				//后面再处理其他的类型
			}
		}
	}
	
	

	//将查询转到spark+parquet执行,但是效率好像是有那么点问题的，出在spark的mapreduce上
	public static void queryFromParquet(String inputFile, String sql) {
		
	}
	
	/*
	public static void queryFromBitmap(String inputFile) throws IOException,ClassNotFoundException {
		String hg00100_00 = BITMAP_DIR + inputFile + "HG00100_0|0";
		String hg00100_10 = BITMAP_DIR + inputFile + "HG00100_1|0";
		String hg00245_01 = BITMAP_DIR + inputFile + "HG00245_0|1";
		String hg00245_11 = BITMAP_DIR + inputFile + "HG00245_1|1";
		String hg00251_01 = BITMAP_DIR + inputFile + "HG00251_0|1";
		String hg00251_11 = BITMAP_DIR + inputFile + "HG00251_1|1";
		
		Configuration hadoopConf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
		
		ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00100_00))));
		BitSet bitSet_hg00100_00 = (BitSet) ins.readObject();
		
		ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00100_10))));
		BitSet bitSet_hg00100_10 = (BitSet) ins.readObject();
		
		ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00245_01))));
		BitSet bitSet_hg00245_01 = (BitSet) ins.readObject();
		
		ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00245_11))));
		BitSet bitSet_hg00245_11 = (BitSet) ins.readObject();
		
		ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00251_01))));
		BitSet bitSet_hg00251_01 = (BitSet) ins.readObject();
		
		ins = new ObjectInputStream(new FSDataInputStream(fs.open(new Path(hg00251_11))));
		BitSet bitSet_hg00251_11 = (BitSet) ins.readObject();

		bitSet_hg00100_00.or(bitSet_hg00100_10);
		bitSet_hg00245_01.or(bitSet_hg00245_11);
		bitSet_hg00251_01.or(bitSet_hg00251_11);
		
		bitSet_hg00245_01.and(bitSet_hg00251_01);
		bitSet_hg00100_00.and(bitSet_hg00245_01);
		
		System.out.println("queryFromBitmap : " + bitSet_hg00100_00.cardinality());
 	}*/
}
