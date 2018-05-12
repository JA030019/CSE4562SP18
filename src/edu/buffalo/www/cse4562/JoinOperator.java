package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class JoinOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression;
	boolean isOpen = true;
	boolean isNatural = false;
	boolean isInner = true;
	boolean isSimple = false;	
	
	ArrayList<Tuple> tupleListR = new ArrayList<Tuple>();
	int count = 0;

	Tuple tempTupleL = new Tuple();;

	Tuple tempTupleR = new Tuple();;	
	
	public JoinOperator(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression) {
	
		this.tl = tl;
        this.tr = tr;
        this.expression = expression;
        this.open();
        this.print();
			
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
			//tr.close();
			isOpen = false;
		}	
		
	}

	@Override
	public Tuple getNext() {
		
		Tuple tupleCombine = null;
		
	
		//write in right tuple into tuplelist
		if(tupleListR.isEmpty()) {
			while(tr.hasNext()) {				
				Tuple temp = tr.getNext();
				if(temp != null) {
				    tupleListR.add(temp);
				}
			
		    }
		}
		
		//inialize right tuple
		if(tempTupleR.fullTupleMap.isEmpty()) {
			tempTupleR = tupleListR.get(0);
		}else {			
			count ++;
			if(count < tupleListR.size()) {
				tempTupleR = tupleListR.get(count);
			}else {
				tempTupleR = null;
			}
		}
		
		//inialize left tuple
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tl.getNext();
		}
		
		//Case 1
		if(tempTupleR == null) {
	      
			//update left tuple 
			tempTupleL = tl.getNext();
			
			//1.1 left, right is null, done
			if(tempTupleL == null) {	
				this.close();
				return null;
				
			}
			
			//1.2 left has new tuple, right is null
			//reset right tableOperator
			count = 0;
			// read from the first tuple of right table
			tempTupleR = tupleListR.get(count);		
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
					
						count ++;
						if(count < tupleListR.size()) {
							tempTupleR = tupleListR.get(count);
						}else {
							tempTupleR = null;
						}
					   
					   //2.2.1 if tuple from table r is not null
					   if(tempTupleR != null) {
							try {
								tupleCombine = joinTuple(tempTupleL, tempTupleR, expression);
							} catch (SQLException e) {
								e.printStackTrace();
							}
					   }
						//2.2.2 if tuple from table r is null
						//reach the end of the r table
					   else	if(tempTupleR == null) {

	   
							// move to next in the left table
							//update left tuple 
							tempTupleL = tl.getNext();
							
							//2.2.2.1 left is null, done
							if(tempTupleL == null) {
								return null;
							}
							
							//reset right tableOperator
							count = 0;
							// read from the first tuple of right table
							tempTupleR = tupleListR.get(count);	
							
							
							//2.2.2.2 left is not null, combine
							 if(tempTupleL != null) {
								try {											
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
		if(count < tupleListR.size()) {
			return true;
		}

		//this.close();
		return false;
	
	}

   public Tuple joinTuple(Tuple t1, Tuple t2, Expression expression) throws SQLException {
		
		//if expression true return tuple else null
		Tuple outTuple = new Tuple();

		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
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

	@Override
	public void print() {
		System.err.println("join one table read");
				
		
	}
	
}
