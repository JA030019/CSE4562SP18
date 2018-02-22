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

public class Tuple {
	
	//key-> coloumn Name, Value-> data in PrimitiveValue
	LinkedHashMap<String,PrimitiveValue> tupleMap;
	
	public Tuple(LinkedHashMap<String,PrimitiveValue> tupleMap ) {
		this.tupleMap = tupleMap;
	}

	//override
	//set the value of data in the tuple
	public void setValue( String columnName, StringValue value) {
		 tupleMap.put(columnName, value);
	}

	public void setValue(String columnName, LongValue value) {		
		
		tupleMap.put(columnName, value);
	}

	public void setValue(String columnName, DoubleValue value) {		
		tupleMap.put(columnName, value);		
	}

	public void setValue(String columnName, DateValue value) {		
		tupleMap.put(columnName, value);		
	}

	public void setValue(String columnName, NullValue value) {
		tupleMap.put(columnName, value);	
		
	}	

  /*  public void printTuple() throws InvalidPrimitive {
    	   	
    	ArrayList<String> outTuple = new ArrayList<>();
	    	if(tupleMap == null) {
	    		return;

	    	}
	    	
			Set<String> columnNames = tupleMap.keySet();		

			for(String str: columnNames) {
				PrimitiveValue value = tupleMap.get(str);					  
			
			    switch(value.getType()){			 
				case BOOL:
				     value.toBool();break;
				case DOUBLE:
					 value.toDouble();break;
				case LONG :	
					 value.toLong();break;
				case STRING:
					 value.toString();break;
				case DATE:               
					 value.toString();break;
				default:
					break;
				}
			  outTuple.add(value.toString());  
		  }     
		    			       	
		for(int j=0; j < outTuple.size(); j++) {
	    	
	    	if(j != outTuple.size()-1) {
	    		System.out.print(outTuple.get(j) + "|");
	    	}else {	    					
			System.out.print(outTuple.get(j));
			System.out.println();
	    	  }
			 
	    }
	}*/

	  public void printTuple() throws InvalidPrimitive {
   	
	ArrayList<String> outTuple = new ArrayList<>();
	
    	if(tupleMap == null) {
    		return;
    	}
    	
    	if(tupleMap.keySet() == null) {
    		return;
    	}
    	
		Set<String> columnNames = tupleMap.keySet();		

		for(String str: columnNames) {
		PrimitiveValue value = tupleMap.get(str);	
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
	
	
	public PrimitiveValue getTupleData(String str) {
		PrimitiveValue temp = null;
		temp = tupleMap.get(str);
		return temp;
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
