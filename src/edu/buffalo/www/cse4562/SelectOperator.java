package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.LinkedHashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class SelectOperator implements TupleIterator<Tuple> {

	TupleIterator<Tuple> to;
	Expression expression;
	boolean isOpen = true;
	Evaluate evaluate; 
	
	public SelectOperator(TupleIterator<Tuple> to, PlainSelect plainSelect) {
		this.to = to;
		this.expression = plainSelect.getWhere();
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
					
			LinkedHashMap<Column,PrimitiveValue> fullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
			Tuple tuple = new Tuple(fullTupleMap);
			
			tuple = to.getNext();
			//System.out.println("selection from table "+ tuple.fullTupleMap.size() +" "+ tuple.fullTupleMap.isEmpty());
			//System.out.println("selection from table "+ tuple.fullTupleMap.get();
			this.evaluate = new Evaluate(tuple);
			
			if(tuple == null) {
				return null;
			}
			
			/*try {
				
				System.out.println("judge A>4" + ((BooleanValue) (evaluate).eval(expression)).getValue());
				System.out.println("----------------");
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
			
		    // test where condition
		    try {
		    	if(expression == null) {
		    		return tuple;
		    	}
		    	else if (((BooleanValue) (evaluate).eval(expression)).getValue()) {
					   return tuple;
				}else {
					return this.getNext();
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

