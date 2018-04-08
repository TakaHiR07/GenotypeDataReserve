package com.takahiro.test;

public class Test {
	public static void main(String[] args) throws Exception{
		long startTime = System.nanoTime();
		Thread.sleep(1000);
		long endTime = System.nanoTime();
		System.out.println((endTime-startTime) / 10000000);
	}
}
