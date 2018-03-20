package edu.buffalo.www.cse4562;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class JoinOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression;
	boolean isOpen = true;
	boolean isNatural = false;
	boolean isInner = true;
	boolean isSimple = false;		
    LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
	Tuple tempTupleL = new Tuple(tempFullTupleMap);	

	public JoinOperator(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression) {
	
		this.tl = tl;
        this.tr = tr;
        this.expression = expression;
        open();
			
	}

	@Override
	public void open() {
		if(!isOpen) {
			tl.open();
			tr.open();
			isOpen = true;
		}
		
	}

	@Override
	public void close() {
		 if(isOpen) {
			tl.close();
			tr.close();
			isOpen = false;
		}	
		
	}

	@Override
	public Tuple getNext() {
		
		/*LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTupleL = new Tuple(tempFullTupleMap);	*/		
		
		LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap2 = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tupleCombine = new Tuple(tempFullTupleMap2);
		
		LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap1 = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tempTupleR = new Tuple(tempFullTupleMap1);		
        
		if(tr.hasNext()) {
        	tempTupleR = tr.getNext();
        }else {
        	tempTupleR = null;
        }		
		
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tl.getNext();
		}
		
	
		
		/*if(al.isEmpty()) {
			tempTupleL = tl.getNext();
			al.add(tempTupleL);
		}else {
			tempTupleL = al.get(0);			
		}*/		

				
		//1. right is null
		if(tempTupleR == null) {
	         
			tempTupleL = tl.getNext();
			
			
			//1.1 left, right is null, done
			if(tempTupleL == null) {	
				this.close();
				return null;
				
			}
			/*//left is not null
			al.remove(0);
			al.add(tempTupleL);*/
			
			//1.2 left has new tuple, right is null
			//reset right tableOperator
			tr.close();
			tr.open();
			// read from the first tuple of right table
			tempTupleR = tr.getNext();		
		}
		
		//2. right table has new tuple
		if (tempTupleR != null) {
			
		    //2.1 right not null, left is null
			if(tempTupleL == null) {
				return null;
			}
			
			//2.2 right and left is not null
			if(tempTupleL != null) {
				
				try {
					tupleCombine = joinTuple(tempTupleL, tempTupleR, expression);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				while(tupleCombine == null) {
					   tempTupleR = tr.getNext();
					   
					   //2.2.1 if tuple from table r is not null
					   if(tempTupleR != null) {
							try {
								tupleCombine = joinTuple( tempTupleL, tempTupleR, expression);
							} catch (SQLException e) {
								e.printStackTrace();
							}
					   }
						//2.2.2 if tuple from table r is null
						//reach the end of the r table
					   else	if(tempTupleR == null) {
						    
						    //reset right tableOperator
							tr.close();
							tr.open();
							// read from the first tuple of right table
							tempTupleR = tr.getNext();
						    
							// move to next in the left table
							tempTupleL = tl.getNext();
							
							//2.2.2.1 left is null, done
							if(tempTupleL == null) {
								return null;
							}
							//2.2.2.2 left is not null, combine
							else if(tempTupleL != null) {
								try {
									
									/*//left is not null
									al.remove(0);
									al.add(tempTupleL);*/
									
									tupleCombine = joinTuple( tempTupleL, tempTupleR, expression);
								} catch (SQLException e) {
									e.printStackTrace();
								} 
							}
					   
					   }

				}//end while
			    return tupleCombine;	
			}
		}	
		return null;
	}

	@Override
	public boolean hasNext() {

		if(tl.hasNext()) {
			return true;
		}else {
			tl.close();
		}
		
		//tl.hasNext() -> false
		if(tr.hasNext()) {
			return true;
		}else {
			tr.close();
		}

		//this.close();
		return false;
	
	}
	
	public Tuple joinTuple(Tuple t1, Tuple t2, Expression expression) throws SQLException {
		
		//if expression true return tuple else null
		LinkedHashMap<Column,PrimitiveValue> outFullTupleMap = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple outTuple = new Tuple(outFullTupleMap);

		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
		/*System.out.println("combined tuple");
		outTuple.printTuple();*/
		 Evaluate evaluate = new Evaluate(outTuple);
		 
		 if(expression == null){
			 return outTuple;		 
		 } else if(((PrimitiveValue) (evaluate).eval(expression))==null) {
	    	 return outTuple;
	     } else if (((BooleanValue) (evaluate).eval(expression)).getValue()) {
			 return outTuple;
		 }
		
		return null;
		
	}
	
	
		
}

