package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class SortGroupbyOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>();
	boolean isOpen = true;
	
	
	@Override
	public void open() {
		if(!isOpen) {
	    	   ti.open();
	    	   isOpen = true;
	       }	
		
	}

	@Override
	public void close() {
		if(isOpen) {
			ti.close();
			isOpen = false;
		}	
		
	}

	@Override
	public Tuple getNext() {
		
		return null;
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

}
