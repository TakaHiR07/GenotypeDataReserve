package com.takahiro.graduation_project.test;

import java.util.BitSet;

import org.roaringbitmap.RoaringBitmap;

//测试证明，的确roaringbitmap在逻辑操作速度上要比没有压缩的bitset快
public class RoaringBitmapTest {
	public static void main(String[] args) {
		testBitset();
		testRoaringBitmap();
	}
	public static void testBitset() {
		long startTime = System.nanoTime();
		BitSet rr = new BitSet();
		BitSet rr2 = new BitSet();
		for (int i = 0; i < 1500000; i++) {
			rr.set(i);
		}
		for (int i = 1000000; i < 2000000; i++) {
			rr2.set(i);;
		}
		long endTime = System.nanoTime();
		System.out.println("Bitset setTime:" + (endTime-startTime));
		startTime = System.nanoTime();
		rr.and(rr2);
		System.out.println(rr.cardinality());
		endTime = System.nanoTime();
		System.out.println("Bitset logi1cTime:" + (endTime-startTime));
	}
	
	public static void testRoaringBitmap() {
		long startTime = System.nanoTime();
		RoaringBitmap rr = new RoaringBitmap();
		RoaringBitmap rr2 = new RoaringBitmap();
		for (int i = 0; i < 1500000; i++) {
			rr.add(i);
		}
		for (int i = 1000000; i < 2000000; i++) {
			rr2.add(i);
		}
		long endTime = System.nanoTime();
		System.out.println("RoaringBitmap setTime:" + (endTime-startTime));
		startTime = System.nanoTime();
		rr.and(rr2);
		System.out.println(rr.getCardinality());
		endTime = System.nanoTime();
		System.out.println("RoaringBitmap logicTime:" + (endTime-startTime));
	}
}
