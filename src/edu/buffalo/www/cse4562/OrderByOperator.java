package edu.buffalo.www.cse4562;

import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class OrderByOperator implements TupleIterator<Tuple> {
		
	
	TupleIterator<Tuple> ti;
	Boolean isOpen = true;
	List<OrderByElement> orderByElements; 
	ArrayList<Tuple> tupleBuffer = new ArrayList<>();
	int count = 0;
	Tuple tuple;
	 	
	public OrderByOperator(TupleIterator<Tuple> ti, List<OrderByElement> orderByElements) {
		this.ti = ti;		
		this.orderByElements = orderByElements;
		this.open();
		this.print();
	}		
	
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
		
		Tuple tempTuple = null;		
		/*//if there is no orderby in query, return tuple
		//if(orderByElements.isEmpty()) { 
		
		if(orderByElements == null) {
		 tempTuple =  ti.getNext();
			if(tempTuple == null) {
				this.close();
				return null;
			}				
			return tempTuple;		
		}*/

		
		// if there is order by in query
		//add all tuples into tuple buffer
		// only do it once!!!!		
		if(tupleBuffer.isEmpty()) {			
			while(ti.hasNext()) {			 	
			    tempTuple = ti.getNext();
			 
				if(tempTuple == null) {
					this.close();
					break;
				}	
			
			    tupleBuffer.add(tempTuple);				 
		   }				
					
		   Collections.sort(tupleBuffer, new SortByExpression(orderByElements));		
		}

		if(count < tupleBuffer.size()) {
			tempTuple = tupleBuffer.get(count);
			count++;
			return tempTuple;
		}
		
		/*if(count >= tupleBuffer.size()){
			count = 0;
		}*/

		return null;
	}

	@Override
	public boolean hasNext() {

		if(ti.hasNext() || count < tupleBuffer.size()) {
			return true;
		}
				
		this.close();
		return false;
	}

	@Override
	public void print() {
		System.err.println("orderby");
		
	}

}

