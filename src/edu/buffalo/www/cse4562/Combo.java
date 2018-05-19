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
	
	public boolean equals(Object o) {
		
		//System.err.println("equals");
		if(this == o) {
			return true;
		}
		
		if(o == null || this.getClass() != o.getClass()) {
			return false;
		}
		
		Combo combo = (Combo) o;
		
		return this.a.equals(combo.a) && this.b.equals(combo.b) && this.comboList.equals(combo.comboList);
		
	}
	
     public int hashCode() {
		
		return this.a.hashCode() + this.b.hashCode();
	}
	
	
	
}
