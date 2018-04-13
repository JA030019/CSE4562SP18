package edu.buffalo.www.cse4562;

import java.util.HashMap;
import java.util.Set;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class SubSelectOperator implements TupleIterator<Tuple>{

	
	TupleIterator<Tuple> to;
	String subSelectAlias;
	boolean isOpen = true;
	Tuple tuple;
	
	public SubSelectOperator(TupleIterator<Tuple> to,String subSelectAlias) {
		this.to = to;
		this.subSelectAlias = subSelectAlias;		
		open();
		this.print();
	}
	

	@Override
	public void open() {
		if(!isOpen) {
	    	   to.open();
	       }	
		
	}

	@Override
	public void close() {
		if(isOpen) {
			  to.close();
		}	
	}

	@Override
	public Tuple getNext() {
		
		if(to.hasNext()) {
					
		/*	HashMap<Column,PrimitiveValue> fullTupleMap = new HashMap<Column,PrimitiveValue>(); 
			Tuple tuple = new Tuple(fullTupleMap);*/
			
			 tuple = to.getNext();
			
			if(tuple == null) {
				return null;
			}
	
			if(subSelectAlias == null) {
				return tuple;
			}
			
			//HashMap<Column,PrimitiveValue> fullTupleMaptemp = new HashMap<Column,PrimitiveValue>(); 
			Tuple tupletemp = new Tuple();
			
			//Set<Column> columns = tuple.fullTupleMap.keySet();			
			for(Column c: tuple.fullTupleMap.keySet()) {
				
				Table t1 = new Table(subSelectAlias);
				//Column c1 = new Column(t1, c.getColumnName().toLowerCase());
				Column c1 = new Column(t1, c.getColumnName());
				//c.getTable().setName(subSelectAlias);		
				tupletemp.fullTupleMap.put(c1, tuple.fullTupleMap.get(c));
			}
					
			return tupletemp;
			
		}
			
		return null;
	}

	@Override
	public boolean hasNext() {
		if(to.hasNext()) {
			return true;
		}
		return false;
	}


	@Override
	public void print() {
		System.err.println("subselect");
		
	}

	
}
