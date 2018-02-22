package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.LinkedHashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

public class SelectOperator implements TupleIterator<Tuple> {

	TableOperator to;
	LinkedHashMap<String,PrimitiveValue> tupleMap = new LinkedHashMap<String,PrimitiveValue>(); 
	Tuple tuple = new Tuple(tupleMap);
	Expression expression;
	boolean isOpen = true;
	Evaluate evaluate; 
	
	public SelectOperator(TableOperator to,Expression expression) {
		this.to = to;
		this.expression = expression;
		this.open();
	}

	@Override
	public void open() {
		if(!isOpen) {
			to.open();
			isOpen = true;
		}
	}

	@Override
	public void close() {
		if(isOpen) {
			to.close();
			isOpen = false;
		}		
	}

	@Override
	public Tuple getNext(){ 

		if(to.hasNext()) {
			tuple = to.getNext();
			this.evaluate = new Evaluate(tuple);
							
		    // test where condition
		    try {
		    	if(expression == null) {
		    		return tuple;
		    	}
		    	else if (((BooleanValue) (evaluate).eval(expression)).getValue()) {
					   return tuple;
				}else {
					return to.getNext();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}		
		return null;
	}

	@Override
	public boolean hasNext() {
		
		if(to.hasNext()) {
			return true;
		}
				
		this.close();
		return false;
	}

}

