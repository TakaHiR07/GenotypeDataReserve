package com.takahiro.graduation_project.test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;

public class Test {
	public static void main(String[] args) throws InterruptedException{
		System.out.println("ABBBBBBBBBBBBVC".toLowerCase());
		System.out.println(true && false);
		System.out.println("a=123".contains("="));
		
		BitSet bitSet = null;
		BitSet bitSet2 = new BitSet();
		bitSet2.set(100);
		Stack<BitSet> stack = new Stack<BitSet>();
		stack.push(bitSet);
		//stack.push(bitSet2);
		while (!stack.isEmpty()) {
			System.out.println(stack.pop());
		}
		
		final List<String> list = new ArrayList<String>();
		list.add("123");
		list.add("23");
		System.out.println(list.size());
		
		long startTime = System.nanoTime();
		Thread.sleep(1000);
		long endTime = System.nanoTime();
		System.out.println(endTime-startTime);
	}
}
