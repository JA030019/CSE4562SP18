package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class PlainSelectParser {
	
	String subSelectAlias;
	PlainSelect plainselect;
	//File file;
	//CreateTable ct;
	ProjectOperator po;	
	
	/*// info from select sentence eg: SELECT A, B, C FROM R AS T WHERER R.A>3;
	Table table;//R AS T
	Expression whereEx;//condition R.A > 3
	List<SelectItem> selectItemList; //A, B, C
*/
	public PlainSelectParser(PlainSelect plainselect) {
		
		this.plainselect = plainselect;
		//this.file = file;
		//this.ct = ct;
		
	}
	
	public ProjectOperator find(PlainSelect plainSelect) throws IOException {
		
		FromItem fromitem = plainSelect.getFromItem();
		if(fromitem instanceof SubSelect) {
			SelectBody sb = ((SubSelect) fromitem).getSelectBody();		
			subSelectAlias = ((SubSelect) fromitem).getAlias();
			
			SubSelectOperator sub = new SubSelectOperator(find((PlainSelect)sb),subSelectAlias);
		    SelectOperator subso = new SelectOperator(sub, plainSelect);//select where
			                  po = new ProjectOperator(subso, plainSelect);//project select
			                  
			                  return po;
			
		}else if(fromitem instanceof Table) {
			
			Table table = (Table)fromitem;										       	 
				
			TableOperator to = new TableOperator(table);//table from //return one tuple from the file and converts datatype to PrimitveValue				                                                 						
			SelectOperator so = new SelectOperator(to,plainSelect);//select where 				
			               po = new ProjectOperator(so,plainSelect);//project select	
		
			               return po;
		        }
		
		return null;	
		
	}
	
}
