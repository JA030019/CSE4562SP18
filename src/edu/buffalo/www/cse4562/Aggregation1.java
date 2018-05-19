package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Aggregation1 {

	Function func;
	String funcName;
	double tempmax = 0;
	double tempmin = 0;
	double tempsum = 0;
	double tempavg = 0;
	int counter = 0;
	int countersum = 0;
	String dataType = null;
	
	PrimitiveValue max;
	PrimitiveValue min;
	PrimitiveValue sum;
	PrimitiveValue avg;
	LongValue count;
	
	String alias = null;
	
	public Aggregation1(Function func, String funcName, String alias) {
		this.func = func;
		this.funcName = funcName;
		this.alias = alias;
		}
	
	public PrimitiveValue getAggregationValue(String funcName) {
		
		//to be modified for *, allColumns -->ONLY COUNT(*)
		if(funcName.equals("MAX")) {
		    return max;
			
		}else if(funcName.equals("MIN")) {
			return min;
			
		}else if(funcName.equals("COUNT")) {
			return count;
			
		}else if(funcName.equals("SUM")) {
			return sum;
			
		}else if (funcName.equals("AVG")) {
			return avg;

		}
		return null;        
	}
	
	public void getAggregation(Tuple tuple) {
		
		//to be modified for *, allColumns -->ONLY COUNT(*)
		if(funcName.equals("*")) {
		    
		}else if(funcName.equals("MAX")) {
		    
			this.max(tuple);
		}else if(funcName.equals("MIN")) {
			
			this.min(tuple);
		}else if(funcName.equals("COUNT")) {
			
			this.count(tuple);
		}else if(funcName.equals("SUM")) {
			
			this.sum(tuple);
		}else if (funcName.equals("AVG")) {
			
			this.avg(tuple);
		}
		
	}
	
	public void max(Tuple tuple) {
        
		counter ++;
		
		//only consider SINGLE expression Max(R.A + R.B*R.C)
		Expression exp = func.getParameters().getExpressions().get(0);

		//get data type
		Evaluate evaluate = new Evaluate(tuple);
		
		//get data type & new a variable
		if(dataType == null) {
			PrimitiveValue temp1 = null;
			if(exp != null) {
				
				try {
				temp1 = evaluate.eval(exp);
			    } 
				catch (SQLException e) {			
				e.printStackTrace();
			    }
				
			}else {
				System.out.println("expression null");
			}
			
			dataType = getDataType(temp1);
		}
		
		//update max value
		double dtemp = 0;
		try {
			dtemp = evaluate.eval(exp).toDouble();
		} catch (Exception e) {			
			e.printStackTrace();
		} 
		
		if(counter == 1) {
			tempmax = dtemp;
		}else {
			if(tempmax < dtemp) {
				tempmax = dtemp;
			}
		}
		
		
		if(dataType.equals("LongValue")) {
			
			max = new LongValue((long)tempmax);
			
		}else {
			max = new DoubleValue(tempmax);
		}
	
	}
	
	public void min(Tuple tuple) {
		
        counter ++;
		
		//only consider SINGLE expression Min(R.A + R.B*R.C)
		Expression exp = func.getParameters().getExpressions().get(0);

		//get data type
		Evaluate evaluate = new Evaluate(tuple);
		
		//get data type & new a variable
		if(dataType == null) {
			PrimitiveValue temp1 = null;
			if(exp != null) {
				
				try {
				temp1 = evaluate.eval(exp);
			    } 
				catch (SQLException e) {			
				e.printStackTrace();
			    }
				
			}else {
				System.out.println("expression null");
			}
			
			dataType = getDataType(temp1);
		}
		
		//update min value
		double dtemp = 0;
		try {
			dtemp = evaluate.eval(exp).toDouble();
		} catch (Exception e) {			
			e.printStackTrace();
		} 
		
		if(counter == 1) {
			tempmin = dtemp;
		}else {
			if(tempmin > dtemp) {
				tempmin = dtemp;
			}
		}
		
        if(dataType.equals("LongValue")) {
			
			min = new LongValue((long)tempmin);
			
		}else {
			min = new DoubleValue(tempmin);
		}
		
	}
	
	public void sum(Tuple tuple) {
		countersum ++;
		
		//only consider SINGLE expression sum(R.A + R.B*R.C)
		Expression exp = func.getParameters().getExpressions().get(0);

		//get data type
		Evaluate evaluate = new Evaluate(tuple);
		
		//get data type & new a variable
		if(dataType == null) {
			PrimitiveValue temp1 = null;
			if(exp != null) {
				
				try {
				temp1 = evaluate.eval(exp);
			    } 
				catch (SQLException e) {			
				e.printStackTrace();
			    }
				
			}else {
				System.out.println("expression null");
			}
			
			dataType = getDataType(temp1);
		}
		
		//update sum value
		double dtemp = 0;
		try {
			dtemp = evaluate.eval(exp).toDouble();
		} catch (Exception e) {			
			e.printStackTrace();
		} 
		
		tempsum = tempsum + dtemp;
		
        if(dataType.equals("LongValue")) {
			
			sum = new LongValue((long)tempsum);
			
		}else {
			sum = new DoubleValue(tempsum);
		}

	}
	
	public void avg(Tuple tuple) {		      
		
		sum(tuple);

		
		tempavg = tempsum/(countersum);
		
        if(dataType.equals("LongValue")) {
			
			avg = new LongValue((long)tempavg);
			
		}else {
			avg = new DoubleValue(tempavg);
		}
	
	}
	
	
	//dont work rn
	public void count(Tuple tuple) {
		
		int tempc = 0;

		tempc ++;
	
		count = new LongValue(tempc);
		
		
	}
	
	//consider all columns in one expression have the same data type
    public String getDataType(PrimitiveValue t){
				
    	PrimitiveType dataType1 = t.getType();
    	String temp1 = null;
    	switch(dataType1){
		case DOUBLE:
			temp1 = "DoubleValue";
			break;
		case LONG:
			temp1 = "LongValue";				 
			break;
		default:			
			break;
		}			      
	
    	return temp1;

	}
	
}