package com.takahiro.test;

public class Bitmap {
	private byte[] bitmap = null;
	public Bitmap(int size) {
		if (size % 8 == 0) {
			bitmap = new byte[size/8];
		} else {
			bitmap = new byte[size/8 + 1];
		}
	}
	
	//set one bit as 1
	public void setTag(int number) {
		int index = 0;
		int bit_index = 0;
		if (number%8 == 0) {
			index = number/8 - 1;
			bit_index = 8;
		} else {
			index = number/8;
			bit_index = number%8;
		}
		switch (bit_index) {
		case 1:
			bitmap[index] = (byte) (bitmap[index]|0x01);
			break;
		case 2:
			
			break;

		default:
			break;
		}
	}
	public static void main(String[] args) {
		Bitmap bitmap = new Bitmap(10);
	}
}
