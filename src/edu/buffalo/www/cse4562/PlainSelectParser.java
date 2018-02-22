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
	
	PlainSelect plainselect;
	File file;
	CreateTable ct;
	
	// info from select sentence eg: SELECT A, B, C FROM R AS T WHERER R.A>3;
	Table table;//R AS T
	Expression whereEx;//condition R.A > 3
	List<SelectItem> selectItemList; //A, B, C

	public PlainSelectParser(PlainSelect plainselect,File file, CreateTable ct) {
		
		this.plainselect = plainselect;
		this.file = file;
		this.ct = ct;
		
		this.table = plainselect.getInto();
		this.whereEx = plainselect.getWhere();
		this.selectItemList = (List<SelectItem>) plainselect.getSelectItems();
	}
	
	public ProjectOperator find(PlainSelect plainSelect) throws IOException {
		
		FromItem fromitem = plainSelect.getFromItem();
		if(fromitem instanceof SubSelect) {
			SelectBody sb = ((SubSelect) fromitem).getSelectBody();
			return find((PlainSelect) sb);
			
		}else if(fromitem instanceof Table) {
			
			Table table = (Table)fromitem;										       	 
				
			TableOperator to = new TableOperator(table,file,ct);//table from //return one tuple from the file and converts datatype to PrimitveValue				                                                 						
			SelectOperator so = new SelectOperator(to, whereEx);//select where 				
			ProjectOperator po = new ProjectOperator(so, selectItemList);//project select	
		
			return po;
		        }
		return null;	
		
	}
	
}
