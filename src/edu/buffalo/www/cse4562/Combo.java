package edu.buffalo.www.cse4562;

import java.util.ArrayList;

public class Combo {

	String a;
	String b;
	ArrayList<String> comboList = new ArrayList<>();
	
	public Combo(String a, String b) {
		this.a = a;
		this.b = b;
		comboList.add(a);
		comboList.add(b);
	}
	
}
