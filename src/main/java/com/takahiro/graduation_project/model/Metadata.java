package com.takahiro.graduation_project.model;

import java.io.Serializable;

public class Metadata implements Serializable{
	private int total;
	
	public void setTotal(int total) {
		this.total = total;
	}
	
	public int getTotal() {
		return total;
	}
}
