package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Optimizer {
	
	HashMap<String , Expression> selectelements = new HashMap<>();    
	
	Expression selRest = null;
	
	//attribute for projection pushing down
	//Key -> table name, Value-> List of selectItem for specific table projection
	HashMap<String, ArrayList<SelectItem>> optProMap = new HashMap<>();

	HashMap<Combo, Expression> hashJoinMap = new HashMap<>();
	
	ArrayList<String> tableList = new ArrayList<>();
	
	boolean hasFunc = false;
	boolean hasGroupby = false;
	
	public Optimizer(List<Column> columnRefList, List<SelectItem> expProList) {
		this.hasFunc = this.hasFunc(expProList);
		this.hasGroupby = (columnRefList == null || columnRefList.isEmpty()) ? false : true;
	}
	
    public ArrayList<Expression> andExpAnalyzer(Expression expression){
		
	    ArrayList<Expression> expList = new ArrayList<>();
	    //single expression
	    if(!( expression instanceof AndExpression)){
	    	expList.add(expression);
	    }
	    
	    while(expression instanceof AndExpression) {
			Expression l = ((AndExpression) expression).getLeftExpression();
			Expression r = ((AndExpression) expression).getRightExpression();
			expList.add(r);
			
			if(l instanceof AndExpression) {
				expression = l;
			}else {
				expList.add(l);
				expression = l;
			}
		}
	    
		return expList;		
	}
    
    public ArrayList<Expression> orExpAnalyzer(Expression expression){
		
	    ArrayList<Expression> expList = new ArrayList<>();
	    //single expression
	    if(!( expression instanceof OrExpression)){
	    	expList.add(expression);
	    }
	    
	    while(expression instanceof OrExpression) {
			Expression l = ((OrExpression) expression).getLeftExpression();
			Expression r = ((OrExpression) expression).getRightExpression();
			expList.add(r);
			
			if(l instanceof OrExpression) {
				expression = l;
			}else {
				expList.add(l);
				expression = l;
			}
		}
	    
		return expList;		
	}

	public ArrayList<Column> binarySplit(Expression expression){
		
		ArrayList<Column> columnList = new ArrayList<Column>();
		if(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if(l instanceof Column) {
				Column columnl = (Column)l;
				columnList.add(columnl);
			}
			
			if(r instanceof Column) {
				Column columnr = (Column)r;
				columnList.add(columnr);
			}		    
			return columnList;
		}				
		return null;		
	}
	
	public ArrayList<Column> allBinarySplit(Expression expression){
		
		ArrayList<Column> columnList = new ArrayList<Column>();
		
		while(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if((l instanceof Column) && (r instanceof Column)) {
				Column columnl = (Column)l;
				columnList.add(columnl);
				Column columnr = (Column)r;
				columnList.add(columnr);
				break;				
			}			
			
		    if((l instanceof Column) && (r instanceof PrimitiveValue)) {
		    	Column columnl = (Column)l;
				columnList.add(columnl);
				break;
		    }
		    
		    if((r instanceof Column) && (l instanceof PrimitiveValue)) {
		    	Column columnr = (Column)r;
				columnList.add(columnr);
				break;
		    }
			
			if(l instanceof Column) {
				Column columnl = (Column)l;
				columnList.add(columnl);
			}
			
			if(r instanceof Column) {
				Column columnr = (Column)r;
				columnList.add(columnr);
			}
			
			if((l instanceof BinaryExpression) && (r instanceof BinaryExpression)) {
				columnList.addAll(allBinarySplit(l));
				columnList.addAll(allBinarySplit(r));
			}
			
			if(l instanceof BinaryExpression) {
				
				expression = l;
			}
			
			if(r instanceof BinaryExpression) {
				
				expression = r;
			}
			
		}
		
		return columnList;
		
	}
	
    public ArrayList<String> binaryAnalyzer(Expression expression){
		ArrayList<String> newtableList = new ArrayList<>();
		ArrayList<String> tableList = new ArrayList<>();
		if(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if(l instanceof BinaryExpression) {
				tableList.addAll(binaryAnalyzer(l));
			}
			
			if(r instanceof BinaryExpression) {
				tableList.addAll(binaryAnalyzer(r));
			}
			
			
			if(l instanceof Column) {
				
				//use tricky way to fix bugs of no alias
				if(((Column) l).getTable().getName() == null) {
					String tableName = null;
	
	    			String na = ((Column) l).getColumnName().split("_")[0];
	    			if(na.equals("P")) {
	    				tableName = "PART";    				 
	    				tableList.add(tableName);
	    			}else if(na.equals("S")) {
	    				tableName = "SUPPLIER";
	    				tableList.add(tableName);
	    			}else if(na.equals("PS")) {
	    				tableName = "PARTSUPP";
	    				tableList.add(tableName);
	    			}else if(na.equals("C")) {
	    				tableName = "CUSTOMER";
	    				tableList.add(tableName);
	    			}else if(na.equals("O")) {
	    				tableName = "ORDERS";
	    				tableList.add(tableName);
	    			}else if(na.equals("L")) {
	    				tableName = "LINEITEM";
	    				tableList.add(tableName);
	    			}else if(na.equals("N")) {
	    				tableName = "NATION";
	    				tableList.add(tableName);
	    			}else if(na.equals("R")) {
	    				tableName = "REGION";
	    				tableList.add(tableName);
	    			}
				}else {
					Column columnl = (Column)l;
				    tableList.add(columnl.getTable().getName());
				}
				
				
			}
			
			if(r instanceof Column) {
				
				//use tricky way to fix bugs of no alias
				if(((Column) r).getTable().getName() == null) {
					String tableName = null;
	
	    			String na = ((Column) r).getColumnName().split("_")[0];
	    			if(na.equals("P")) {
	    				tableName = "PART";    				 
	    				tableList.add(tableName);
	    			}else if(na.equals("S")) {
	    				tableName = "SUPPLIER";
	    				tableList.add(tableName);
	    			}else if(na.equals("PS")) {
	    				tableName = "PARTSUPP";
	    				tableList.add(tableName);
	    			}else if(na.equals("C")) {
	    				tableName = "CUSTOMER";
	    				tableList.add(tableName);
	    			}else if(na.equals("O")) {
	    				tableName = "ORDERS";
	    				tableList.add(tableName);
	    			}else if(na.equals("L")) {
	    				tableName = "LINEITEM";
	    				tableList.add(tableName);
	    			}else if(na.equals("N")) {
	    				tableName = "NATION";
	    				tableList.add(tableName);
	    			}else if(na.equals("R")) {
	    				tableName = "REGION";
	    				tableList.add(tableName);
	    			}
				}else {
					Column columnr = (Column)r;
				    tableList.add(columnr.getTable().getName());
				}
			}		    						
			
			
	    	Iterator<String> it = tableList.iterator();
	    	while(it.hasNext()){          
	    	        String s = it.next();       
	    	        if(!newtableList.contains(s)){      
	    	        	newtableList.add(s);       
	    	        }
	    	}
			
			
			return newtableList;
		}				
		return null;
		
	}	
    
    
 
    public boolean containsEqual(Expression exp) {   	
    	if(exp instanceof EqualsTo) {
    		return true;
    	}    	
		return false;    	
    }
    
    public boolean containsNonEquals(Expression exp) {
    	if(exp instanceof GreaterThan) {
    		return true;
    	}    	
    	if(exp instanceof GreaterThanEquals) {
    		return true;
    	}
    	if(exp instanceof MinorThan) {
    		return true;
    	}
    	
    	if(exp instanceof MinorThan) {
    		return true;
    	}
    	
    	if(exp instanceof MinorThanEquals) {
    		return true;
    	}
    	
    	if(exp instanceof NotEqualsTo) {
    		return true;
    	}
		return false;
		
    }
    
    public Expression combineExpression(ArrayList<Expression> expressionList) {		
    	if(!expressionList.isEmpty()) {   		  
    		 // BooleanValue t = BooleanValue.TRUE;
			  Expression temp = expressionList.get(0);
			  if(expressionList.size() == 1) {
				  return temp;
			  }else {
				  for(int i = 1; i < expressionList.size(); i++) {
					  Expression extemp = expressionList.get(i);
					  temp = new AndExpression(temp, extemp);
				  }
				  return temp;				  
			  }
        }  	
    	return null;    	
    }
 
    public ArrayList<SelectItem> setSelectItemAlias(List<SelectItem> expProList){   	    
    	ArrayList<SelectItem> temp = new ArrayList<>();
    	for(SelectItem s: expProList) {  
    		
    		if(s instanceof AllTableColumns) {
    			temp.add(s);
    		}    		 		
    		
    		if(s instanceof SelectExpressionItem) {    	
    			SelectExpressionItem st = new SelectExpressionItem();
				Expression exp = ((SelectExpressionItem) s).getExpression();
				
				//case of function eg: SUM(R.A + R.B*R.C) AS Q
				if(exp instanceof Function) {
					Function function = (Function) exp;
	    			ExpressionList eList = function.getParameters();
	    			List<Expression> expList =  eList.getExpressions();
	    			
	    			if(expList != null) {
	    				for(Expression ex : expList) {	    					
	    					st.setExpression(ex);
	    					temp.add(st);
	    				}
	    			}	    			
				}
				//case of nonfunction expression eg: R.A
				else {
					st.setExpression(exp);   		
				    temp.add(st);
				}
				
    	    }    		
    	}   	
    	
    		return temp;
    	  	
    }
    
    //combine selectItem list of projection and selelction
    public ArrayList<SelectItem> combine(ArrayList<SelectItem> proList, ArrayList<Expression> optSelList, List<Column> columnRefList){
    	
    	//ArrayList<Expression> temp = new ArrayList<>();
    	for(Expression exp: optSelList) {
    		SelectExpressionItem tempItem = new SelectExpressionItem();
    		tempItem.setExpression(exp);
    		proList.add(tempItem);	
    	}
    	
    	if(columnRefList != null) {
    		for(Column c: columnRefList) {
    		SelectExpressionItem tempItem = new SelectExpressionItem();
    		tempItem.setExpression(c);
    		proList.add(tempItem);
    	    }
    	}  	
    		return proList;
    	    	
    }
    
    public void optPro(ArrayList<SelectItem> proList) {   	
    	//namelist storing tablename of alltablecolumns T.* -> T for filtering T.C
    	ArrayList<String> nameList = new ArrayList<>();
    	ArrayList<Column> columnList = new ArrayList<>();
    	
    	for(SelectItem temp : proList) {
    		
    		//case1 put alltablecolumns into hashmap
    		if(temp instanceof AllTableColumns) {
    			ArrayList<SelectItem> tempList = new ArrayList<>();
    			Table table = ((AllTableColumns) temp).getTable();
    			String tableName = table.getName();
    			tempList.add(temp);
    			optProMap.put(tableName, tempList);
    			nameList.add(tableName);	
    		}
    		//case2
    		else if(temp instanceof SelectExpressionItem) {
    			Expression expression = ((SelectExpressionItem) temp).getExpression();
    			
    			//case2.1 function SUM(R.A + R.B*R.C)
    			if(expression instanceof Function) {
    				Function function = (Function) expression;
        			List<Expression> el = function.getParameters().getExpressions();
        			//only consider one expression el.get(0)
        			ArrayList<Column> cl = allBinarySplit(el.get(0));
        			for(Column c: cl) {
        				String talbeName = c.getTable().getName();
        				//if there is alltablecolumns,delete corresponding other columns T.*, T.C
            			//add all columns to columnlist
            			if(!nameList.contains(talbeName)) {
            				columnList.add(c);
            			}		
        			}	
    			}

    			//case 2.2 expression R.A + R.B
    			ArrayList<Column> cl1 = allBinarySplit(expression);
    			for(Column c: cl1) {
    				String talbeName = c.getTable().getName();
    				//if there is alltablecolumns,delete corresponding other columns T.*, T.C
        			//add all columns to columnlist
        			if(!nameList.contains(talbeName)) {
        				columnList.add(c);
        			}		
    			}
    			
    			//case2.3 column R.A
    			if(expression instanceof Column) {
    				Column column = (Column) expression;
        			String talbeName = column.getTable().getName();
       			
        			//if there is alltablecolumns,delete corresponding other columns T.*, T.C
        			//add all columns to columnlist
        			if(!nameList.contains(talbeName)) {
        				columnList.add(column);
        			}	
    			}
    					
    		}
    	}
    	
    	//remove duplicated columns 
        //newColumnList (no duplicated columns)
    	ArrayList<Column> newColumnList = new ArrayList<>();
    	Iterator<Column> it = columnList.iterator();
    	while(it.hasNext()){          
    	        Column c = it.next();       
    	        if(!newColumnList.contains(c)){      
    	        	newColumnList.add(c);       
    	        }
    	}
    	
    	//cluster columns into groups according table name
    	ArrayList<String> tableNameList = new ArrayList<>();
    	
    	//tricky method for fixing bugs of the no alias case
    	ArrayList<Column> clist = new ArrayList<>();
    	
    	//save the table names
    	for(Column c : newColumnList) { 
    		
    		//bug of the optimization method, tricky way to fix it for checkpoint 4
    		if(c.getTable().getName() == null) {
    			
    			String tableName = null;
    			
    			Column col;
    			String na = c.getColumnName().split("_")[0];
    			if(na.equals("P")) {
    				tableName = "PART";    				 
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("S")) {
    				tableName = "SUPPLIER";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("PS")) {
    				tableName = "PARTSUPP";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("C")) {
    				tableName = "CUSTOMER";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("O")) {
    				tableName = "ORDERS";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("L")) {
    				tableName = "LINEITEM";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("N")) {
    				tableName = "NATION";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}else if(na.equals("R")) {
    				tableName = "REGION";
    				col = new Column(new Table(tableName),c.getColumnName());
    				clist.add(col);
    			}
    			
    			if(!tableNameList.contains(tableName)) {
	    			tableNameList.add(tableName);
	    		}
    			
    		}
    		else {
    			
	    		if(!tableNameList.contains(c.getTable().getName())) {
	    			tableNameList.add(c.getTable().getName());
	    		}	
    		}
    		
    		
    	}
    	
    	// 1. put columns into arraylist according to table name
    	// 2. put tablename and corresponding column arraylist into hashmap
    	
    	/*//bug case there is only one table, column without table name
    	//eg: Select A, B from R WHERE B>3;
    	if(tableNameList.contains(null)) {
    		ArrayList<SelectItem> selectItemList = new ArrayList<>();
    		for(Column c : newColumnList) {
    			SelectExpressionItem st = new SelectExpressionItem();
				st.setExpression(c);
				selectItemList.add(st);
    		}
    		
    		//optProMap.put(s, selectItemList);
    		
    	}else {*/
    	
    	
    	if(clist.isEmpty()) {
    		if(!tableNameList.contains(null)) {
    	        for(String s: tableNameList) {    		
    		        ArrayList<SelectItem> selectItemList = new ArrayList<>();
	    		    for(Column c : newColumnList) {  		
		    			if(s.equals(c.getTable().getName())) {
		    				
		    				SelectExpressionItem st = new SelectExpressionItem();
		    				st.setExpression(c);
		    				selectItemList.add(st);
		    			}
	    			
	    		    }
    	    
    	            optProMap.put(s, selectItemList);
    	        }		
    	    }
    	}else {
    		for(String s: tableNameList) {    		
		        ArrayList<SelectItem> selectItemList = new ArrayList<>();
    		    for(Column c : clist) {  		
	    			if(s.equals(c.getTable().getName())) {
	    				
	    				SelectExpressionItem st = new SelectExpressionItem();
	    				st.setExpression(c);
	    				selectItemList.add(st);
	    			}
    			
    		    }
	    
	            optProMap.put(s, selectItemList);
	        }
    		
    	}
    	
    	    
    	    
    	
	   	
    }
    
    
    public boolean hasFunc(List<SelectItem> selectItems) {
		
		boolean hasFunc = false;
		
		for(SelectItem s : selectItems) {
			if(s instanceof SelectExpressionItem) {
				Expression expression = ((SelectExpressionItem) s).getExpression();	
				if(expression instanceof Function) {
					hasFunc = true;
					break;
				}
			}
		}
		
		return hasFunc;
		
	}
}

