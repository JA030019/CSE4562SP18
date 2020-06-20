package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class ColMix {

	//String alias;
	PrimitiveValue data;
	Column column;
	
	
	public ColMix(PrimitiveValue data, Column column) {
		this.data = data;
		this.column = column;
	}
	
}
