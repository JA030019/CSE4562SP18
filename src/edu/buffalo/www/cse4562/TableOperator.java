package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class TableOperator implements TupleIterator<Tuple>{
			
	Table table;	    
	CreateTable ct;
	File file;	
	BufferedReader reader = null;
	String filepath;
	
	public TableOperator(Table table) {
		this.table = table;
		this.filepath= "data/"+ table.getName()+ ".dat"; 
		this.file = new File(filepath);
		this.ct = Main.tableMap.get(table.getName().toLowerCase());	
		this.open();
		this.print();		
	}
	
	public void print() {
		System.err.println("Table " + table.getName());
	}
	

	@Override
	public void open() {
		if(reader == null) {
			try {
				reader = new BufferedReader(new FileReader(file));	
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
	    if (reader != null) {	    	
	        try {
	        	reader.close();
	        	reader = null;
	        } 
	        catch (IOException e) {
	          e.printStackTrace();
	        }
	        
	    }
    }

	@Override
	public Tuple getNext() {
	
		Tuple tuple = new Tuple();
				
		if(reader == null) {
			return null;
		}
		
        String line = null;
		
		try {
			line = reader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
		if(line == null) {
			return null;
		}
		
		String[] columns = line.split("\\|");
				
		List<ColumnDefinition> columnDefinitions = ct.getColumnDefinitions();
		
		for(int i = 0; i< columns.length ;i++) {
			String dataType = columnDefinitions.get(i).getColDataType().getDataType().toLowerCase();
			String columnName = columnDefinitions.get(i).getColumnName();
			
			switch(dataType){
			case "string" :
			case "varchar": 
			case "char":
			    tuple.setValue(table, columnName, new StringValue(columns[i]));break;
			case "int":
				tuple.setValue(table, columnName, new LongValue(Long.parseLong(columns[i])));
				break;
			case "integer":
				tuple.setValue(table, columnName, new LongValue(Long.parseLong(columns[i])));
				break;
			case "double":
				tuple.setValue(table, columnName, new DoubleValue(Double.parseDouble(columns[i])));				 
				break;
			case "decimal":				 
				tuple.setValue(table, columnName, new DoubleValue(Double.parseDouble(columns[i])));break;
			case "date":
				tuple.setValue(table,columnName, new DateValue(columns[i]));break;
			default:
				tuple.setValue(table, columnName, new NullValue());break;
			}			      
		}
		
		if(table.getAlias() != null){
			tuple.setTableAlias(tuple, table);
		}	     
		
		return tuple;
		
	}

	@Override
	public boolean hasNext() {
		
		 if (reader == null) { 
			 return false;
			 }
		 
		 try {
			 if (reader.ready()) {
			        return true;
			  } else {
				  reader.close();
			        return false;
			  }  
	         } 
		 catch (IOException e) {
				      e.printStackTrace();
				      return false;
				   } 			 
	}


}
	


