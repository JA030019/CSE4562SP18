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
	
	//<Table name,<column name, value>>
	LinkedHashMap<Column,PrimitiveValue> fullTupleMap;
	
	public Tuple() {
		this.fullTupleMap = new LinkedHashMap<>();			
		
	}
		
	//Overload
	//set the value of data in the tuple
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
   	
		 // ArrayList<String> outTuple = new ArrayList<>();
		    StringBuilder outTuple = new StringBuilder();
		  
	    	if(fullTupleMap.isEmpty()) {
	    		return;
	    	}
	    	
	    	if(fullTupleMap.keySet().isEmpty()) {
	    		return;
	    	}
	    	
			Set<Column> columns = fullTupleMap.keySet();		
	
			for(Column c: columns) {
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
		
	/*public PrimitiveValue getTupleData(Table table, String columnName) {
		PrimitiveValue temp = null;
		Column c = new Column(table, columnName);
		temp = fullTupleMap.get(c);
		return temp;
	} */   
	
	public Table getTupleTable() {
		
		if(fullTupleMap == null) {
    		return null;
    	}
    	
    	if(fullTupleMap.keySet() == null) {
    		return null;
    	}
    	
    	ArrayList<Table> outTable = new ArrayList<>();
		Set<Column> columns = fullTupleMap.keySet();
		for(Column c: columns) {
			Table table = c.getTable();	
			if(table != null ) {
				outTable.add(table);  
			}else {
				return null;
			}
		  }   
		
		return outTable.get(0);
		
	}   
	
	
	public Tuple setAlias(Tuple tuple, Table table) {
		
		String tableAlias = null;
		tableAlias = table.getAlias();
		
		//HashMap<Column,PrimitiveValue> fullTupleMaptemp = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tupletemp = new Tuple();
		
		Set<Column> columns = tuple.fullTupleMap.keySet();			
		for(Column c: columns) {						
			c.getTable().setName(tableAlias);		
			tupletemp.fullTupleMap.put(c, tuple.fullTupleMap.get(c));
		}
		
		return tupletemp;
		
	}
	
}
