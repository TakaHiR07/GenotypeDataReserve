package com.takahiro.graduation_project.model;

import java.io.Serializable;
import java.util.Arrays;

public class BitSet implements Cloneable, Serializable{
	private Long[] words;
	private final static int ADDRESS_BITS_PER_WORD = 6;
	private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
	private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;
	private static final long WORD_MASK = 0xffffffffffffffffL;
	
	private transient boolean sizeIsSticky = false;
	
	
	private transient int wordsInUse = 0;
	
	private static int wordIndex(int bitIndex) {
		return bitIndex >> ADDRESS_BITS_PER_WORD;
	}
	
	public BitSet(int nbits) {
		words = new Long[wordIndex(nbits-1) + 1];
		sizeIsSticky = false;
	}
	
	public void expandTo(int wordIndex) {
		int wordsRequired = wordIndex + 1;
		if (wordsInUse < wordsRequired) {
			ensureCapacity(wordsRequired);
			wordsInUse = wordsRequired;
		}
	}
	public void ensureCapacity(int wordsRequired) {
		if (words.length < wordsRequired) {
			int request = Math.max(2 * words.length, wordsRequired);
			words = Arrays.copyOf(words, request);
			sizeIsSticky = false;
		}
	}
	
	public void set(int bitIndex) {
		int wordIndex = wordIndex(bitIndex);
		expandTo(wordIndex);
		words[wordIndex] |= (1L << bitIndex);
	}
	
	public static void main(String[] args) {
		System.out.println(WORD_MASK);
		int size = 1024 >> 6 + 1;
		System.out.println(size);
		
		System.out.println(1L << 10000);
		byte b = 10;
		System.out.println(b);
	}
	
	
}
