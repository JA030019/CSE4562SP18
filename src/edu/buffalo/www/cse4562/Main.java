package edu.buffalo.www.cse4562;

import java.util.*;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVParser;

import java.io.Reader;
import java.io.InputStreamReader;
import net.sf.jsqlparser.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
   	
	static String prompt = "$> "; // expected prompt
	public static HashMap<String,CreateTable> tableMap = new HashMap<>();
	public static HashMap<String, TableInfo2> fullIndexMap = new HashMap<>();
	//Tricky way to optimize for checkpoint 4
	public static HashMap<String, Optimizer> optimizerMap = new HashMap<>();
	
	public static void main(String[] argsArray) throws Exception{					
		
		System.out.println(prompt);
		System.out.flush();
		
		Reader in = new InputStreamReader(System.in);		
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement statement;

        while((statement = parser.Statement()) != null){     
        	
        	long startTime = System.currentTimeMillis(); //long endTime=System.
        	
		     if(statement instanceof CreateTable) {				        		
			      CreateTable ct = new CreateTable();
			      ct = (CreateTable)statement;
			      String tableName = ct.getTable().toString();
			      
			      tableMap.put(tableName.toLowerCase(), ct);	
			      
			      TableInfo2 tableinfo = new TableInfo2(tableName,ct);
			      tableinfo.Parserkey();
			      tableinfo.indexBuilder();
			      //tableinfo.indexToDisk(tableinfo.indexBuilder());   
			      fullIndexMap.put(tableName, tableinfo);
      
			 }else if(statement instanceof Select) {
				       SelectBody sb = ((Select) statement).getSelectBody();				        		
				       
				       TreeBuilder treebuilder = new TreeBuilder(sb);
				       
				       TupleIterator<Tuple> t = treebuilder.treeParser(sb);
				       
				       while(t.hasNext()) {
		        			try {
		        				Tuple tuple = t.getNext();		        				
		        				if(tuple != null) {
		        					tuple.printTuple();
		        				}											
							} catch (Exception e) {
								e.printStackTrace();
							}
	        			}
			 }          		
		     // read for next query
             System.out.println(prompt);
             System.out.flush();
             long endTime = System.currentTimeMillis(); 
             System.err.println("Time = " + (endTime -startTime)); 	
        }
	}
	
 
}


