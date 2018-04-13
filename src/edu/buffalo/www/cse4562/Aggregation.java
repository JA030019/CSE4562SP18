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
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Aggregation {

	Function func;
	String funcName;
	//Evaluate evaluate;
	//boolean hasGroupby = false;
	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();	
	
	public Aggregation(Function func, String funcName, ArrayList<Tuple> tupleList) {
		this.func = func;
		this.funcName = funcName;
		this.tupleList = tupleList;
		//this.hasGroupby = hasGroupby;
	}
	
	
	public PrimitiveValue getAggregation() {
		
		//to be modified for *, allColumns -->ONLY COUNT(*)
		if(funcName.equals("*")) {
		    return null;
		}else if(funcName.equals("MAX")) {
		    System.out.println("MAX");
		    return max();
		}else if(funcName.equals("MIN")) {
			System.out.println("MIN");
			return min();
		}else if(funcName.equals("COUNT")) {
			System.out.println(funcName);
			return count();
		}else if(funcName.equals("SUM")) {
			return sum();
		}else if (funcName.equals("AVG")) {
			return avg();
		}else {
			return null;
		}
		
	}
	
	public PrimitiveValue max() {

		//only consider SINGLE expression Max(R.A + R.B*R.C)
		Expression exp = func.getParameters().getExpressions().get(0);
		 
		//output value
		PrimitiveValue output = null;
		
		//get data type
		//HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple();
		tuple = tupleList.get(0);		
		Evaluate tempev = new Evaluate(tuple);
		PrimitiveValue temp = null;
		String dataType = null;

		//get data type & new a variable
		if(exp != null) {
			try {
			temp = tempev.eval(exp);
		    } catch (SQLException e) {
			
			e.printStackTrace();
		    }
		}else {
			System.out.println("expression null");
		}
		
		dataType = getDataType(temp);
		
		//tempvalue
		DoubleValue p = (DoubleValue) temp;
		
        Double d = null;

			for(Tuple tp: tupleList) {
				Evaluate ev = new Evaluate(tp);
				try {
					d = ev.eval(exp).toDouble();
				} catch (Exception e) {
					System.out.print("aggregation no eval");
					e.printStackTrace();
				}
				
				if( p.getValue() < d) {
					p = new DoubleValue(d);
				}
				
			}
			
			if(dataType.equals("LongValue")) {
				output = new LongValue((long) p.toDouble());
			}else {
				output = p;
			}					
		
		return output;
		
	}
	
	public PrimitiveValue min() {
		
		//only consider SINGLE expression Max(R.A + R.B*R.C)
		Expression exp = func.getParameters().getExpressions().get(0);
		 
		//output value
		PrimitiveValue output = null;
		
		//get data type
		//HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple();
		tuple = tupleList.get(0);		
		Evaluate tempev = new Evaluate(tuple);
		PrimitiveValue temp = null;
		String dataType = null;

		//get data type & new a variable
		if(exp != null) {
			try {
			temp = tempev.eval(exp);
		    } catch (SQLException e) {
			
			e.printStackTrace();
		    }
		}else {
			System.out.println("expression null");
		}
		
		dataType = getDataType(temp);
		
		//tempvalue
		DoubleValue p = (DoubleValue) temp;
		
        Double d = null;		

			for(Tuple tp: tupleList) {
				Evaluate ev = new Evaluate(tp);
				try {
					d = ev.eval(exp).toDouble();
				} catch (Exception e) {
					System.out.print("aggregation no eval");
					e.printStackTrace();
				}
				
				if( p.getValue() > d) {
					p = new DoubleValue(d);
				}
				
			}

			if(dataType.equals("LongValue")) {
				output = new LongValue((long) p.toDouble());
			}else {
				output = p;
			}			

		
		return output;				
		
	}
	
	public PrimitiveValue sum() {
				
	    Expression exp = func.getParameters().getExpressions().get(0);
		 
		//output value
		PrimitiveValue output = null;
		
		//get data type
		//HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple();
		tuple = tupleList.get(0);		
		Evaluate tempev = new Evaluate(tuple);
		PrimitiveValue temp = null;
		String dataType = null;

		//get data type & new a variable
		if(exp != null) {
			try {
			temp = tempev.eval(exp);
		    } catch (SQLException e) {
			
			e.printStackTrace();
		    }
		}else {
			System.out.println("expression null");
		}
		
		dataType = getDataType(temp);
		
		//tempvalue
		DoubleValue p = new DoubleValue(0);
		
        Double d = (double) 0.0;
		
			for(Tuple tp: tupleList) {
				Evaluate ev = new Evaluate(tp);
				try {
					d = d + ev.eval(exp).toDouble();
				} catch (Exception e) {
					System.out.print("aggregation no eval");
					e.printStackTrace();
				}
			}
			p = new DoubleValue(d);
			
			if(dataType.equals("LongValue")) {
				output = new LongValue((long) p.toDouble());
			}else {
				output = p;
			}			

		return output;			
		
	}
	
	public PrimitiveValue avg() {
		
	    Expression exp = func.getParameters().getExpressions().get(0);
		
	    int counterAvg = 0;
		//output value
		PrimitiveValue output = null;
		
		//get data type
		//HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple();
		tuple = tupleList.get(0);		
		Evaluate tempev = new Evaluate(tuple);
		PrimitiveValue temp = null;
		String dataType = null;

		//get data type & new a variable
		if(exp != null) {
			try {
			temp = tempev.eval(exp);
		    } catch (SQLException e) {
			
			e.printStackTrace();
		    }
		}else {
			System.out.println("expression null");
		}
		
		dataType = getDataType(temp);
		
		//tempvalue
		DoubleValue p = new DoubleValue(0);
		
        Double d = (double) 0;
		
			counterAvg = tupleList.size();
			for(Tuple tp: tupleList) {
				Evaluate ev = new Evaluate(tp);
				try {
					d = d + ev.eval(exp).toDouble();
				} catch (Exception e) {
					System.out.print("aggregation no eval");
					e.printStackTrace();
				}
			}
			
			d = d/counterAvg;
			
			p = new DoubleValue(d);
			
			if(dataType.equals("LongValue")) {
				output = new LongValue((long) p.toDouble());
			}else {
				output = p;
			}			
	
		return output;
		
	}
	
	public LongValue count() {
		
		int tempc = 0;

		tempc = tupleList.size();
	
		LongValue counter = new LongValue(tempc);
		
		return counter;
		
	}
	
	//consider all columns in one expression have the same data type
    public String getDataType(PrimitiveValue t){
				
    	PrimitiveType dataType1 = t.getType();
    	String temp1 = null;
    	switch(dataType1){
		/*case BOOL:		
		    temp = t.hashCode();
		    break;
		case DATE:
			temp = t.hashCode();
			break;*/
		case DOUBLE:
			temp1 = "DoubleValue";
			break;
		case LONG:
			temp1 = "LongValue";				 
			break;
		/*case STRING:
			temp = t.hashCode();
			break;
		case TIME:
			temp = t.hashCode();
			break;*/
		default:			
			break;
		}			      
	
    	return temp1;

	}
	
}
