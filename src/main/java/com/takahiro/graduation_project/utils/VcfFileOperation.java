package com.takahiro.graduation_project.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class VcfFileOperation {
	//将头文件切分成只有前300列
	public static void headerSplit300(String inputFile, String outputFile) throws IOException {
		File file1 = new File(inputFile);
		File file2 = new File(outputFile);
		
		BufferedReader reader = new BufferedReader(new FileReader(file1));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file2));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] groups = line.split("\t");
			for (int i = 1; i < groups.length-1 && i < 300; i++) {
				writer.append(groups[i] + "\t");
			}
			writer.append(groups[300] + "\n");
			break;
		}
		reader.close();
		writer.close();	
	}
	
	//将300列的头文件去掉其他，只留下基因型头
	public static void headerSplit(String inputFile, String outputFile) throws IOException {
		File file1 = new File(inputFile);
		File file2 = new File(outputFile);
		
		BufferedReader reader = new BufferedReader(new FileReader(file1));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file2));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] groups = line.split("\t");
			for (int i = 8; i < groups.length-1; i++) {
				writer.append(groups[i] + "\t");
			}
			writer.append(groups[groups.length-1] + "\n");
			break;
		}
		reader.close();
		writer.close();	
	}
	
	//将300列数据文件去掉其他，只留下基因型数据
	public static void dataSplit(String inputFile, String outputFile) throws IOException{
		File file1 = new File(inputFile);
		File file2 = new File(outputFile);
		
		BufferedReader reader = new BufferedReader(new FileReader(file1));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file2));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] groups = line.split("\t");
			for (int i = 8; i < groups.length-1; i++) {
				writer.append(groups[i] + "\t");
			}
			writer.append(groups[groups.length-1] + "\n");
		}
		reader.close();
		writer.close();	
	}
	
	//将头和单个数据文件合并起来
	public static void mergeHeaderAndData(String headerFile, String dataFile, String outputFile) throws IOException{
		File file1 = new File(headerFile);
		File file2 = new File(dataFile);
		File file3 = new File(outputFile);
		
		BufferedReader reader1 = new BufferedReader(new FileReader(file1));
		BufferedReader reader2 = new BufferedReader(new FileReader(file2));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file3));
		String line = null;
		while ((line = reader1.readLine()) != null) {
			writer.append(line + "\n");
		}
		while ((line = reader2.readLine()) != null) {
			writer.append(line + "\n");
		}
		reader1.close();
		reader2.close();
		writer.close();	
	}
	
	//将头和多个数据文件合并起来
	public static void mergeHeaderAndDatas(String headerFile, String outputFile, String ...dataFiles) throws IOException{
		File file1 = new File(headerFile);
		File file3 = new File(outputFile);
		
		BufferedReader reader1 = new BufferedReader(new FileReader(file1));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file3));
		String line = null;
		while ((line = reader1.readLine()) != null) {
			writer.append(line + "\n");
		}
		reader1.close();
		for (String dataFile : dataFiles) {
			BufferedReader reader = new BufferedReader(new FileReader(dataFile));
			while ((line = reader.readLine()) != null) {
				writer.append(line + "\n");
			}
			reader.close();
		}
		writer.close();	
	}
	
	public static void main(String[] args) throws IOException {
		//headerSplit300("/media/sf_Desktop/毕业设计/new.vcf", "/media/sf_Desktop/毕业设计/chr1_header.vcf");
		//headerSplit("/media/sf_Desktop/毕业设计/chr1_header.vcf", "/media/sf_Desktop/毕业设计/chr1_header_forBitmap.vcf");
		/*mergeHeaderAndData("/media/sf_Desktop/毕业设计/chr1_header.vcf", "/media/sf_Desktop/毕业设计/chr1_split3noheader_1000.vcf", 
				"/media/sf_Desktop/毕业设计/chr1_test.vcf");*/
		/*mergeHeaderAndDatas("/media/sf_Desktop/毕业设计/chr1_header.vcf","/media/sf_Desktop/毕业设计/chr1_test.vcf",
				"/media/sf_Desktop/毕业设计/chr1_split3noheader_1000.vcf", "/media/sf_Desktop/毕业设计/chr1_split3header.vcf");*/
		
		
		String operation = args[0];
		if (operation.equals("headerSplit300")) {
			headerSplit300(args[1], args[2]);
		} else if (operation.equals("headerSplit")) {
			headerSplit(args[1], args[2]);
		} else if (operation.equals("dataSplit")) {
			dataSplit(args[1], args[2]);
		} else if (operation.equals("mergeHeaderAndData")) {
			mergeHeaderAndData(args[1], args[2], args[3]);
		} else if (operation.equals("mergeHeaderAndDatas")) {
			int count = args.length;
			String[] groups = new String[count-3];
			for (int i = 0; i < count-3 ; i++) {
				groups[i] = args[i+3];
			}
			mergeHeaderAndDatas(args[1], args[2], groups);
		}
	}
}
