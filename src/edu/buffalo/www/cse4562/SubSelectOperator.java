package edu.buffalo.www.cse4562;

import java.util.HashMap;
import java.util.Set;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class SubSelectOperator implements TupleIterator<Tuple>{

	
	TupleIterator<Tuple> ti;
	String subSelectAlias;
	boolean isOpen = true;	
	
	public SubSelectOperator(TupleIterator<Tuple> ti,String subSelectAlias) {
		this.ti = ti;
		this.subSelectAlias = subSelectAlias;		
		this.open();
		this.print();
	}
	

	@Override
	public void open() {
		if(!isOpen) {
	    	ti.open();
	       }	
		
	}

	@Override
	public void close() {
		if(isOpen) {
		   ti.close();
		}	
	}

	@Override
	public Tuple getNext() {
		
		if(ti.hasNext()) {
			
			Tuple tuple = ti.getNext();
			
			if(tuple == null) {
				return null;
			}
	
			if(subSelectAlias == null) {
				return tuple;
			}
			
			Tuple tempTuple = new Tuple();
			
			for(Column c: tuple.fullTupleMap.keySet()) {				
				Table t1 = new Table(subSelectAlias);
				Column c1 = new Column(t1, c.getColumnName());	
				tempTuple.fullTupleMap.put(c1, tuple.fullTupleMap.get(c));
			}
					
			return tempTuple;
			
		}
			
		return null;
	}

	@Override
	public boolean hasNext() {
		if(ti.hasNext()) {
			return true;
		}else {
			return false;
		}
		
	}


	@Override
	public void print() {
		System.err.println("subselect");
		
	}

	
}
