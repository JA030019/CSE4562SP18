package edu.buffalo.www.cse4562;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;

public class Tuple {     
		
	LinkedHashMap<Column,PrimitiveValue> fullTupleMap;
	
	public Tuple() {
		this.fullTupleMap = new LinkedHashMap<>();			
		
	}
		
	//Overload
	//set value of data in the tuple
	public void setValue( Table table, String columnName, PrimitiveValue value) {
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}
	
	public void setValue(Table table, String columnName, StringValue value) {
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(Table table, String columnName, LongValue value) {		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(Table table, String columnName, DoubleValue value) {		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);		
	}

	public void setValue(Table table, String columnName, DateValue value) {		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);	
	}

	public void setValue(Table table, String columnName, NullValue value) {
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
		
	}
	
	public void setValue(String columnName, PrimitiveValue value) {
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}
	
	public void setValue(String columnName, StringValue value) {
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(String columnName, LongValue value) {		
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue( String columnName, DoubleValue value) {		
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);		
	}

	public void setValue(String columnName, DateValue value) {		
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(String columnName, NullValue value) {
		Table table = new Table();
		
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
		
	}
	
	public void setValue(PrimitiveValue value) {
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}
	
	public void setValue(StringValue value) {
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(LongValue value) {		
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(DoubleValue value) {		
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);		
	}

	public void setValue(DateValue value) {		
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
	}

	public void setValue(NullValue value) {
		Table table = new Table();
		String columnName = null;
		Column c = new Column(table, columnName);
		fullTupleMap.put(c, value);
		
	}


    public void printTuple() throws InvalidPrimitive {
   			 
		    StringBuilder outTuple = new StringBuilder();
		  
	    	if(fullTupleMap.isEmpty() || fullTupleMap == null) {
	    		return;
	    	}	    		    	
	    	
			Set<Column> columnList = fullTupleMap.keySet();		
	
			for(Column c: columnList) {
			    PrimitiveValue value = fullTupleMap.get(c);	
			if(value != null ) {
				outTuple.append(value.toString()).append("|");  
			}else {
				return;
			}
		  }     
		    			       	
		  outTuple.deleteCharAt(outTuple.length()-1);
		  System.out.println(outTuple.toString());
    }
			
	
	public Tuple setTableAlias(Tuple tuple, Table table) {
		
		String tableAlias = null;
		tableAlias = table.getAlias();

		Tuple tempTuple = new Tuple();
		
		Set<Column> columnList = tuple.fullTupleMap.keySet();			
		for(Column c: columnList) {						
			c.getTable().setName(tableAlias);		
			tempTuple.fullTupleMap.put(c, tuple.fullTupleMap.get(c));
		}
		
		return tempTuple;
		
	}
	
}
