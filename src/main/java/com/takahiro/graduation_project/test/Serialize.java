package com.takahiro.graduation_project.test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.BitSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Serialize {
	private static String HADOOP_URI = "hdfs://mu01:9000/";

	public static void main(String[] args) throws IOException,ClassNotFoundException {
		writeObject();
		readObject();
	}
	public static void writeObject() throws IOException{
		Configuration hadoopConf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
		String filename = "hdfs://mu01:9000/user/fanjy/SerializeTest";
		Path path = new Path(filename);
		ObjectOutputStream oos = new ObjectOutputStream(new FSDataOutputStream(fs.create(path)));
		BitSet bitSet = new BitSet(32);
		for (int i = 0; i < 1000000; i++) {
			bitSet.set(i);
		}
		oos.writeObject(bitSet);
		oos.close();
		fs.close();
	}
	
	public static void readObject() throws IOException, ClassNotFoundException{
		Configuration hadoopConf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(HADOOP_URI), hadoopConf);
		String filename = "hdfs://mu01:9000/user/fanjy/SerializeTest";
		Path path = new Path(filename);
		ObjectInputStream ins = new ObjectInputStream(new FSDataInputStream(fs.open(path)));
		BitSet bitSet = (BitSet) ins.readObject();
		System.out.println(bitSet.cardinality());
		ins.close();
		fs.close();
	}
}
