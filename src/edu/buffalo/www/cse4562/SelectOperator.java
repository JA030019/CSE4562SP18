package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SelectOperator implements TupleIterator<Tuple> {

	TupleIterator<Tuple> to;
	List<SelectItem> selectItemList = new ArrayList<SelectItem>(); //SELECT A, B, C
	Expression expression;
	boolean isOpen = true;
	Evaluate evaluate; 
	
	public SelectOperator(TupleIterator<Tuple> to, PlainSelect plainSelect) {
		this.to = to;
		this.expression = plainSelect.getWhere();
		this.selectItemList = plainSelect.getSelectItems();	
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
			//Evaluate evaluate = new Evaluate(tuple);
			
			tuple = to.getNext();
			//System.out.println("selection from table "+ tuple.fullTupleMap.size() +" "+ tuple.fullTupleMap.isEmpty());
			//System.out.println("selection from table "+ tuple.fullTupleMap.get();
			this.evaluate = new Evaluate(tuple);
			
			if(tuple == null) {
				return null;
			}	

			/*LinkedHashMap<Column,PrimitiveValue> fullTupleMaptemp = new LinkedHashMap<Column,PrimitiveValue>(); 
			Tuple tempTuple = new Tuple(fullTupleMaptemp);			
			
			for(SelectItem s: selectItemList) {
				 if(s instanceof SelectExpressionItem) {
					Expression expression = ((SelectExpressionItem) s).getExpression();
					
					String alias = null;
					if(((SelectExpressionItem) s).getAlias() != null) {
						 alias = ((SelectExpressionItem) s).getAlias().toLowerCase();//projection alias name	
						//System.out.println("alias "+ alias);
					}																
					
					 Evaluate evaluate = new Evaluate(tuple);			
					 try {
					    	if(expression == null) {
					    		 return tuple;
					    	}
					    	else {				    		
					    		 if(alias != null) {
					    			 
					    			 System.out.println(alias);
					    			 System.out.println(tuple.getTupleTable());
					    			 System.out.println((PrimitiveValue)(evaluate).eval(expression));
					    			 tempTuple.setValue(tuple.getTupleTable(),alias,(PrimitiveValue)(evaluate).eval(expression));
					    		 }else {
					    			 tempTuple.setValue(tuple.getTupleTable(),((Column) (expression)).getColumnName().toLowerCase(),(PrimitiveValue)(evaluate).eval(expression));
					    		  }
							     }
						} catch (SQLException e) {
							e.printStackTrace();
							return null;
						}		
				}
					
			}
			
			
			
			if( tempTuple.fullTupleMap.isEmpty()) {
				 evaluate = new Evaluate(tuple);
			}else {
				 evaluate = new Evaluate(tempTuple);
			}*/
			
					
		    // test where condition
		    try {
		    	if(expression == null) {
		    		return tuple;
		    	}
		    	else if(((PrimitiveValue) (evaluate).eval(expression))==null) {
		    		return null;
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

