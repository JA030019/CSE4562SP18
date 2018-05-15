package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectOperator implements TupleIterator<Tuple> {

	TupleIterator<Tuple> ti;
	Expression expWhere;
	boolean isOpen = true;
	Evaluate evaluate; 
	Tuple tuple;
	
	public SelectOperator(TupleIterator<Tuple> ti, Expression expWhere) {
		this.ti = ti;
		this.expWhere = expWhere;
		this.open();
		//this.print();
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
	public Tuple getNext(){ 

		    while(true) {

			tuple = ti.getNext();
			
			this.evaluate = new Evaluate(tuple);
			
			if(tuple == null) {
				return null;
			}							
					
		    // test where condition
		    try {
		    	if(expWhere == null) {
		    		return tuple;
		    	}
		    	else if(((PrimitiveValue) (evaluate).eval(expWhere)) == null) {
		    		return null;
		    	}
		    	else if (((BooleanValue) (evaluate).eval(expWhere)).getValue()) {
		    		
					   return tuple;
				}
		    	/*else {
					return this.getNext();
				}*/
			} catch (Exception e) {
				e.printStackTrace();
			}			
		 }		
		//return null;
	}

	@Override
	public boolean hasNext() {
		
		if(ti.hasNext()) {
			return true;
		}				
		this.close();
		return false;
	}


	@Override
	public void print() {
		System.err.println("select" );		
	}

}

