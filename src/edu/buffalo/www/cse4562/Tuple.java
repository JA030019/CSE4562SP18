package edu.buffalo.www.cse4562;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class Tuple {
    
	//LinkedHashMap<String,PrimitiveValue> tupleMap;
	
	//<Table name,<column name, value>>
	 LinkedHashMap<Column,PrimitiveValue> fullTupleMap;
	
	public Tuple(LinkedHashMap<Column,PrimitiveValue> fullTupleMap) {
		this.fullTupleMap = fullTupleMap;			
	}

	//override
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
		//System.out.println("mapsize "+fullTupleMap.size());
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

	  public void printTuple() throws InvalidPrimitive {
   	
		  ArrayList<String> outTuple = new ArrayList<>();
		  		 
		  
	    	if(fullTupleMap == null) {
	    		return;
	    	}
	    	
	    	if(fullTupleMap.keySet() == null) {
	    		return;
	    	}
	    	
			Set<Column> columns = fullTupleMap.keySet();		
	
			for(Column c: columns) {
			PrimitiveValue value = fullTupleMap.get(c);	
			if(value != null ) {
				outTuple.add(value.toString());  
			}else {
				return;
			}
		  }     
		    			       	
		for(int j=0; j < outTuple.size(); j++) {
	    	
	    	if(j != outTuple.size()-1) {
	    		System.out.print(outTuple.get(j) + "|");
	    	}else {	    					
			System.out.print(outTuple.get(j));
			System.out.println();
	    	  }
			 
	    }
}
		
	public PrimitiveValue getTupleData(Table table, String columnName) {
		PrimitiveValue temp = null;
		Column c = new Column(table, columnName);
		temp = fullTupleMap.get(c);
		return temp;
	}
    
	
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
    /*// another try???
    @SuppressWarnings("unchecked")
	public void printTuple(){
    	
    	ArrayList<PrimitiveValue> outTuple = new ArrayList<>();
    	
    	if(tuple == null) {
    		System.out.println("null");
    	}
    	
    	System.out.println("output tuple size " + tuple.size());
		for(int i = 0; i< tuple.size();i++) {
			
			//Hashtable<String,PrimitiveValue> temp = (Hashtable<String, PrimitiveValue>) tuple.get(i);			
			//Set<String> columnNames = temp.keySet();
		    PrimitiveValue value = tuple.get(columnNames.toArray()[0]);					    					       
		    outTuple.add(value);			       
		}
    	
    	Set<String> columnNames = tuple.keySet();
		
		System.out.println(columnNames.size());
	   
		for(String str: columnNames) {
			outTuple.add(tuple.get(str));
		};
    		
		for(int j = 0; j < outTuple.size(); j++) {	    	
	    	if(j != 0 || j != outTuple.size()-1) {
	    		System.out.print("|");
	    	}
	    	System.out.print(outTuple.get(j)); 
	    }
	}*/
	
}
