package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

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

public class ProjectOperator implements TupleIterator<Tuple>{
	
	SelectOperator so;
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C
	Evaluate evaluate;
	boolean isOpen = true;
	
	public ProjectOperator( SelectOperator so, List<SelectItem> selectItems) {
		this.so = so;
		this.selectItems = selectItems;		
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

        LinkedHashMap<Column,PrimitiveValue> fullTupleMap = new  LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple(fullTupleMap);				
		tuple = so.getNext();//get tuple from selectoperator
		
		//System.out.println("projection from selection "+ tuple.fullTupleMap.size() +" "+ tuple.fullTupleMap.isEmpty());
		if(tuple == null) {		
			this.close();
			return null;			
		}		
	
		
		LinkedHashMap<Column,PrimitiveValue> fullTupleMaptemp = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(fullTupleMaptemp);		
		
		for(SelectItem s: selectItems) {
			if(s instanceof AllTableColumns ) {
				
				Table table = ((AllTableColumns) s).getTable();
				
				Set<Column> columns = tuple.fullTupleMap.keySet();
				for(Column c: columns) {
					Table temp = c.getTable();
					if(table.getName().equals(temp.getName())) {
						tempTuple.fullTupleMap.put(c, tuple.fullTupleMap.get(c));
					}
				}
				
			}else if(s instanceof AllColumns) {				
				return tuple;
			}else if(s instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) s).getExpression();
				
				//System.out.println("project table name "+((Column)expression).getTable().getName());
				
				String alias = null;
				if(((SelectExpressionItem) s).getAlias() != null) {
					 alias = ((SelectExpressionItem) s).getAlias().toLowerCase();//projection alias name	
				}																
				
				evaluate = new Evaluate(tuple);				
				 try {
				    	if(expression == null) {
				    		 return tuple;
				    	}
				    	else {				    		
				    		 if(alias != null) {
				    			 
				    			 
				    			 tempTuple.setValue(((Column) expression).getTable(),alias,(PrimitiveValue)(evaluate).eval(expression));
				    			// tempTuple.setValue(tuple.getTupleTable(),alias,(PrimitiveValue)(evaluate).eval(expression));
				    		 }else {
				    			 
				    			 tempTuple.setValue(((Column) expression).getTable(),((Column) (expression)).getColumnName().toLowerCase(),(PrimitiveValue)(evaluate).eval(expression));
				    		  }
						     } 
					} catch (SQLException e) {
						e.printStackTrace();
						return null;
					}		
			}
				
		}	
		
		
		/*try {
			System.out.println("project");
			tempTuple.printTuple();
		} catch (InvalidPrimitive e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
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

