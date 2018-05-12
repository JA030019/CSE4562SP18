package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortByExpression implements Comparator<Tuple>{

	//Expression expression;
	List<OrderByElement> orderByElements;
	//boolean asc = true;
	
	public SortByExpression(List<OrderByElement> orderByElements) {
		this.orderByElements = orderByElements;	
	}	

   @Override
   public int compare(Tuple t1, Tuple t2) {

		int temp = 0;		
		int count = 0;
		
		int sizeOfElements = orderByElements.size();		
		
	    while(temp == 0 && count < sizeOfElements) {
	    	
			 temp = compareHelp(t1,t2,orderByElements.get(count));
			 count++;
			
		}
		return temp;

		
	}
	
	public int compareHelp(Tuple t1, Tuple t2, OrderByElement orderByElement) {				
		
		int help = 0;
		Evaluate evaluate1 = new Evaluate(t1);
		Evaluate evaluate2 = new Evaluate(t2);
		Expression expression = orderByElement.getExpression();
		Column c = (Column)expression;
		
		boolean asc = orderByElement.isAsc();		

		PrimitiveType dataType = null;
		try {
			dataType = (evaluate1).eval(expression).getType();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		switch(dataType){
		case BOOL :
			 break;
		case DATE :
			 DateValue d1 = (DateValue) t1.fullTupleMap.get(c);
			 DateValue d2 = (DateValue) t2.fullTupleMap.get(c);
			
			 if(d1.getYear() == d2.getYear()) {
				 if(d1.getMonth() == d2.getMonth()) {
					 if(d1.getDate() == d2.getDate()) {
						 help = 0;
					 }else {
						 help = d1.getDate() - d2.getDate();
					 }
				 }else {
					 help = d1.getMonth() - d2.getMonth();
				 }
				 
			 }else {
				 help = d1.getYear() - d2.getYear();
			 } 
			 break;
		case DOUBLE :
			  try {
				 help = (int) t1.fullTupleMap.get(c).toDouble() - (int)t2.fullTupleMap.get(c).toDouble();
				//help = (int)((evaluate1).eval(expression).toDouble()-(evaluate2).eval(expression).toDouble());
			} catch (SQLException e1) {
				e1.printStackTrace();
			}break;
		case LONG :			   
			 try {
				//help = (int)((evaluate1).eval(expression).toLong()-((evaluate2).eval(expression).toLong()));
			    help = (int) t1.fullTupleMap.get(c).toLong() - (int)t2.fullTupleMap.get(c).toLong();
			 } catch (SQLException e) {
				e.printStackTrace();
			}break;
		case STRING :				 
			    help = t1.fullTupleMap.get(c).toString().compareTo(t2.fullTupleMap.get(c).toString());
			   //help = (evaluate1).eval(expression).toString().compareTo((evaluate2).eval(expression).toString());break;
		/*case TIME :
		    ;break;*/
		default:
		    break;
		}
				
		if(!asc) {
			help = Math.negateExact(help);
		}
		
		return help;
			
	}
	
}
