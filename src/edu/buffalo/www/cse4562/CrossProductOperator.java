package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class CrossProductOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression;
	boolean isOpen = true;	
	
	ArrayList<Tuple> tupleListR = new ArrayList<Tuple>();
	int countR = 0;
	
	ArrayList<Tuple> tupleListL = new ArrayList<Tuple>();
	int countL = 0;
    	
	Tuple tempTupleL = new Tuple();

	Tuple tempTupleR = new Tuple();	
	
	public CrossProductOperator(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression) {
	
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
			countR ++;
			if(countR < tupleListR.size()) {
				tempTupleR = tupleListR.get(countR);
			}else {
				tempTupleR = null;
			}
		}				
	
		//write in left tuple into tuplelist
		if(tupleListL.isEmpty()) {
			while(tl.hasNext()) {				
				Tuple temp = tl.getNext();
				if(temp != null) {
				    tupleListL.add(temp);
				}
			
		    }
		}
		
		//inialize left tuple		
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tupleListL.get(0);
		}
				
		//Case 1
		if(tempTupleR == null) {
	      
			//update left tuple 
			countL ++;
			if(countL < tupleListL.size()) {
				tempTupleL = tupleListL.get(countL);
			}else {
				tempTupleL = null;
			}
			
			//1.1 left, right is null, done
			if(tempTupleL == null) {	
				this.close();
				return null;
				
			}
			
			//1.2 left has new tuple, right is null
			//reset right tableOperator
			countR = 0;
			// read from the first tuple of right table
			tempTupleR = tupleListR.get(countR);		
		}
		
		
		//2. right table has new tuple
		if (tempTupleR != null) {
			
		    //2.1 right not null, left is null
			if(tempTupleL == null) {
				return null;
			}
			
			//2.2 right and left is not null
			if(tempTupleL != null) {
				
				tupleCombine = joinTuple(tempTupleL, tempTupleR);

			    return tupleCombine;	
			}
	    }				
		
		return null;
	}

	@Override
	public boolean hasNext() {
		
		if(countL == 0 || countL < tupleListL.size()) {
			return true;
		}				
		
		//tl.hasNext() -> false
		if( countL == tupleListL.size() && countR < tupleListR.size()) {
			return true;
		}

		//this.close();
		return false;
	
	}

	public Tuple joinTuple(Tuple t1, Tuple t2){

		Tuple outTuple = new Tuple();
		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
		
		return outTuple;		
	}

	@Override
	public void print() {
		System.err.println("cross product");
		
	}

	
}

