package com.takahiro.graduation_project.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class VcfFileTransform {
	public static void main(String[] args) throws IOException{
		String inputFile = args[0];
		String outputFile = args[1];
		fileToBitmapFile(inputFile, outputFile);
	}
	
	public static void fileToBitmapFile(String inputFile,String outputFile) throws IOException {
		File file1 = new File(inputFile);
		File file2 = new File(outputFile);
		
		BufferedReader reader = new BufferedReader(new FileReader(file1));
		BufferedWriter writer = new BufferedWriter(new FileWriter(file2));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] groups = line.split("\t");
			for (int i = 1; i < groups.length-1; i++) {
				writer.append(groups[i] + "\t");
			}
			writer.append(groups[groups.length-1] + "\n");
		}
		reader.close();
		writer.close();	
	}
}
