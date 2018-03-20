package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

public class Optimizer {
	
	//HashMap<Expression, ArrayList<Column>> optelements = new HashMap<>(); 
	HashMap< String , Expression> optelements = new HashMap<>(); 

    public ArrayList<Expression> exprssionAnalyzer(Expression expression){
		
	    ArrayList<Expression> expList = new ArrayList<>();
	    while(expression instanceof AndExpression) {
			Expression l = ((AndExpression) expression).getLeftExpression();
			Expression r = ((AndExpression) expression).getRightExpression();
			expList.add(l);
			
			if(r instanceof AndExpression) {
				expression = r;
			}else {
				expList.add(r);
				expression = r;
			}
		}
	    
		return expList;		
	}
	
	
	/*public ArrayList<Column> binaryAnalyzer(Expression expression){
		
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
		
	}*/
	
    public ArrayList<String> binaryAnalyzer(Expression expression){
		
		ArrayList<String> tableList = new ArrayList<>();
		if(expression instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) expression).getLeftExpression();
			Expression r = ((BinaryExpression) expression).getRightExpression();
			
			if(l instanceof Column) {
				Column columnl = (Column)l;
				tableList.add(columnl.getTable().getName());
			}
			
			if(r instanceof Column) {
				Column columnr = (Column)r;
				tableList.add(columnr.getTable().getName());
			}
		    
			return tableList;
		}		
		
		return null;
		
	}
	
	
}

