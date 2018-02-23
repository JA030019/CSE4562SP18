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
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
   	
	static String prompt = "$> "; // expected prompt
	static File file = new File("./data/R.dat");
	public static void main(String[] argsArray) throws Exception{
		
		System.out.println(prompt);
		System.out.flush();
		
		Reader in = new InputStreamReader(System.in);		
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement statement;
         // project here
         while((statement = parser.Statement()) != null){       	 				         			        		            			    
		     if(statement instanceof CreateTable) {				        		
			      CreateTable ct = (CreateTable)statement;
				  new createTable(ct);
				  createTable.createTableMap();						        		
			 }else if(statement instanceof Select) {
				   SelectBody sb = ((Select) statement).getSelectBody();				        		
				        		
				   if(sb instanceof PlainSelect) {			        			
	        		   PlainSelect plainSelect = (PlainSelect)sb;	        			
	        		   PlainSelectParser plainSelectParser = new PlainSelectParser(plainSelect,file,createTable.ct);	        			
	        		   ProjectOperator po = plainSelectParser.find(plainSelect);
	
	        			while(po.hasNext()) {
		        			try {
		        				Tuple tuple = po.getNext();		        				
		        				if(tuple != null) {
		        					tuple.printTuple();
		        				}											
							} catch (Exception  e) {
								e.printStackTrace();
							}
	        			}
				     }else if(sb instanceof Union) {
				            System.out.println("Union");			
				     }else {
				            System.out.println("can't handle it");
				        	}
			}          
	         System.out.println();		
		     // read for next query
             System.out.println(prompt);
             System.out.flush();
             
          		
        }
		
	}
    
}



