package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class createTable {	
   	
	//key -> table name, value -> table information
	// tableInfo: key -> columnNumber, value -> columnName 
	public static HashMap<String,HashMap<Integer,String>> fulltablemap = new HashMap<>();
	
	//key -> table name, value -> createTable object
	public static HashMap<String, CreateTable> simpleTableMap = new HashMap<>();
	
	public static CreateTable ct;
	
	public createTable( CreateTable ct) {
		this.ct = ct;
	}
	
	public static void createTableMap() {
		
		HashMap<Integer,String> tableInfo = new HashMap<>();
		
		//get columndefinition consisting of columnName
		List<ColumnDefinition> columnDefinitions = ct.getColumnDefinitions();
	    int columnNumber = 0;
		
		for (ColumnDefinition col :columnDefinitions) {
		    tableInfo.put(columnNumber, col.getColumnName());
			columnNumber++;
		}
		
		fulltablemap.put(ct.getTable().getName().toString().toLowerCase(), tableInfo);
		simpleTableMap.put(ct.getTable().getName().toString().toLowerCase(), ct);
		
	}
}

