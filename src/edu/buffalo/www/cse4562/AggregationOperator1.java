package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggregationOperator1 implements TupleIterator<Tuple>{

	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>(); // columns for group by
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C	
	boolean isOpen = true;
	
	boolean hasFunc = false;
	boolean hasGroupby = false;
	
	int countercase = 0;
	
	public AggregationOperator1(TupleIterator<Tuple> ti, Expression expHaving, List<Column> columnRefList, List<SelectItem> selectItems, Optimizer op) {
		this.ti = ti;
		this.expHaving = expHaving;
		this.columnRefList = columnRefList;
		this.selectItems = selectItems;
		this.hasFunc = op.hasFunc;
		this.hasGroupby = op.hasGroupby;
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
		
		countercase++;

		//HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple();	
		
		//HashMap<Column,PrimitiveValue> fullTupleMaptemp3 = new HashMap<Column,PrimitiveValue>(); 
		Tuple outputTuple = new Tuple();
		
		ArrayList<Aggregation1> l = new ArrayList<>();
	
		for(SelectItem s: selectItems) {		
						
			if(s instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) s).getExpression();				
				
				String alias = null;
				if(((SelectExpressionItem) s).getAlias() != null) {
					 alias = ((SelectExpressionItem) s).getAlias();//projection alias name	
				}										
				
				//parser function
				//Case1.3.1 function : a) count() b) max() c) min() d) avg() e)distinct()?
				if(expression instanceof Function) {
					Function function = (Function) expression;
					String functionName = function.getName();
				
					Aggregation1 aggregation = new Aggregation1(function, functionName, alias);
					
					l.add(aggregation);
																
				}
											
			}
				
		}
		
		while(ti.hasNext()) {			 	
		    tuple = ti.getNext();//get tuple from projectOperator	
		    
			if(tuple == null) {
				this.close();
				break;
			}	
			
			for(Aggregation1 a : l) {
				a.getAggregation(tuple);				
			}
		}
		
		for(Aggregation1 a : l) {
			if(a.alias != null) {
				 outputTuple.setValue(a.alias,a.getAggregationValue(a.funcName));
			}else {
				String fake = a.funcName + a;
				outputTuple.setValue(fake,a.getAggregationValue(a.funcName));
			}
			
		}
		
		return outputTuple;	
	
	}

	@Override
	public boolean hasNext() {
		
		if(countercase == 0 ) {
			return true;
		}
		return false;
	}


	@Override
	public void print() {
		System.err.println("AggregationOperator no groupby");
		
	}

}
