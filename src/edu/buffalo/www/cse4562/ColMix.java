package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class ColMix {

	String alias;
	PrimitiveValue data;
	Column column;
	
	public ColMix(String alias,PrimitiveValue data, Column column) {
		this.alias = alias;
		this.data = data;
		this.column = column;
	}
	
}
