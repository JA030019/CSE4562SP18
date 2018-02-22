package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator implements TupleIterator<Tuple>{
	
	LinkedHashMap<String,PrimitiveValue> tupleMap = new LinkedHashMap<String,PrimitiveValue>(); 
	Tuple tuple= new Tuple(tupleMap);
	SelectOperator so;
	List<SelectItem> selectItemList = new ArrayList<SelectItem>(); //SELECT A, B, C
	Evaluate evaluate;
	boolean isOpen = true;
	
	public ProjectOperator( SelectOperator so, List<SelectItem> seletitemlsit) {
		this.so = so;
		this.selectItemList = seletitemlsit;		
		this.open();
	}

	@Override
	public void open() {
       if(!isOpen) {
    	   so.open();
       }		
	}

	@Override
	public void close() {
		if(isOpen) {
			so.close();
		}		
	}

	@Override
	public Tuple getNext() {
		
		tuple = so.getNext();//get tuple from selectoperator		
		if(tuple == null) {		
			this.close();
			return null;			
		}		
		LinkedHashMap<String,PrimitiveValue> tupleMap1 = new LinkedHashMap<String,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(tupleMap1);//create tempTuple for output 
		for(SelectItem s: selectItemList) {
			if(s instanceof AllTableColumns ) {
				return tuple;
			}else if(s instanceof AllColumns) {				
				return tuple;
			}else if(s instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) s).getExpression();
				String alias = ((SelectExpressionItem) s).getAlias();
				evaluate = new Evaluate(tuple);				
				 try {
				    	if(expression == null) {
				    		 return tuple;
				    	}
				    	else {
				    		 
				    		 if(expression instanceof Column) {  //CASE SELECT A,B,C				    			
				    			 tempTuple.tupleMap.put(((Column) (expression)).getColumnName().toLowerCase(),(PrimitiveValue)(evaluate).eval(expression));
				    		 
				    		 }else if(alias != null) {         //case SELECT A + B AS Q
				    				 tempTuple.tupleMap.put(alias,(PrimitiveValue)(evaluate).eval(expression));				    			 				    			 
				    		         }				    		
						     }
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}		
			}
				
		}		
		return tempTuple;
	}

	@Override
	public boolean hasNext() {
		
		if(so.hasNext()) {
			return true;
		}
		
		close();
		return false;
	}
			

}

