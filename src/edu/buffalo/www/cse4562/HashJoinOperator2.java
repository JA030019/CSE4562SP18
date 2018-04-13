package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;

public class HashJoinOperator2 implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression; // L.A = R.C only contains equal symbol
	boolean isOpen = true;
	
	ArrayList<Tuple> tupleListR = new ArrayList<Tuple>();// store all tuples of right table
	
	ArrayList<Tuple> tupleListL = new ArrayList<Tuple>();// store all tuples of left table
	int countL = 0;
	
	//LinkedHashMap<Column,PrimitiveValue> tempFullTupleMap5 = new LinkedHashMap<Column,PrimitiveValue>(); 
	//Tuple tempTupleL = new Tuple(tempFullTupleMap5);	

	ArrayList<Tuple> tupleHashList = new ArrayList<Tuple>();
	int count = 0;// flag the position of tuple in the tupleHashList
	
	HashMap<Integer, ArrayList<Tuple>> hashcodeMap = new HashMap<>();
	
	//HashMap<Column,PrimitiveValue> tempFullTupleMap1 = new HashMap<Column,PrimitiveValue>(); 
	Tuple tempTupleL = new Tuple();
	
	//Integer hashcodeL = new Integer(0); 
	int hashcodeL = 0;
	
	//HashMap<Column,PrimitiveValue> tempFullTupleMap = new HashMap<Column,PrimitiveValue>(); 
	Tuple tempTupleR = new Tuple();	
	
	//HashMap<Column,PrimitiveValue> tempFullTupleMap3 = new HashMap<Column,PrimitiveValue>(); 
	Tuple temp = new Tuple();	
	
	Column targetColumn = null;
	
	public HashJoinOperator2 (TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression) {
	
		this.tl = tl;
        this.tr = tr;
        this.expression = expression;
        open();		
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
			//tl.close();
			//tr.close();
			isOpen = false;
		}			
	}

	@Override
	public Tuple getNext() {

		//HashMap<Column,PrimitiveValue> tempFullTupleMap2 = new HashMap<Column,PrimitiveValue>(); 
		Tuple tupleCombine = new Tuple();			
		
		//get columns
		ArrayList<Column> columnList =  expressionAnalyzer(expression);
			
		//write in right tuple into tuplelist
		//calculate hashcode for targeted column and build hashcodemap<hashcode, list<tuple>>
		if(hashcodeMap.isEmpty()) {
			//long startTime=System.currentTimeMillis(); //long endTime=System.
			while(tr.hasNext()) {
				Tuple tuple = tr.getNext();
				if(tuple != null) {
					
					//get targetColumn for join (R.A)
					if(targetColumn == null) {
						for(Column c: columnList) {
							if(tuple.fullTupleMap.containsKey(c)) {
								targetColumn = c;
								break;
							}
					    }
					}
					
					//get hashcode and build hashcodeMap
					int hashCode = hashCodeCalculator(tuple.fullTupleMap.get(targetColumn));
					if(!hashcodeMap.containsKey(hashCode)) {
						ArrayList<Tuple> atmp = new ArrayList<Tuple>();
						atmp.add(tuple);
						hashcodeMap.put(hashCode, atmp);
						
					}else {						
						ArrayList<Tuple> atmp = hashcodeMap.get(hashCode);
						atmp.add(tuple);
						hashcodeMap.put(hashCode, atmp);						
					}
				}
			}
			
			 //long endTime = System.currentTimeMillis(); 
             //System.out.println("Time = " + (endTime -startTime)); 
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
		
		columnList.remove(targetColumn);
		
		//inialize left tuple and get corresponding hashcode (first time)		
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tupleListL.get(0);
			if(tempTupleL != null) {				
				 hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(columnList.get(0)));							
			}
		}
		
		//HashJoin
		while(!hashcodeMap.containsKey(hashcodeL)) {
            
			//update left tuple 
			countL ++;
			if(countL < tupleListL.size()) {
				tempTupleL = tupleListL.get(countL);
			}else {
				tempTupleL = null;
			}
			
            if(tempTupleL == null) {
            	return null;
            }           

		     hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(columnList.get(0)));		
		}
		
		if(hashcodeMap.containsKey(hashcodeL)) {
			tupleHashList = hashcodeMap.get(hashcodeL);
			if(!tupleHashList.isEmpty() && tupleHashList != null) {	
				//update tuple to be joined from the hashtuplelist
				if(count < tupleHashList.size()) {
					 temp = tupleHashList.get(count);					
				     count ++;
				}else {
					// count = 0;
					 temp = null;
				}			   												
				//join
				if(temp != null) {
					tupleCombine = joinTuple(tempTupleL,temp);
					return tupleCombine;
				}				

				if(temp == null) {
					
					//update left tuple 
					countL ++;
					if(countL < tupleListL.size()) {
						tempTupleL = tupleListL.get(countL);
					}else {
						tempTupleL = null;
					}
					
					if(tempTupleL == null) {
		            	return null;
		            }
					
					count = 0;
					
				    hashcodeL = hashCodeCalculator(tempTupleL.fullTupleMap.get(columnList.get(0)));		
					
					return this.getNext();
				}
			}
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		
		if(countL == 0 || countL < tupleListL.size()) {
			return true;
		}		
		
		if(tempTupleL == null) {
			return false;
		}
		
		//tl.hasNext() -> false
		if(countL == tupleListL.size() && count < tupleHashList.size()) {
			return true;
		}

		//this.close();
		return false;
	}

    public Tuple joinTuple(Tuple t1, Tuple t2){

		//HashMap<Column,PrimitiveValue> outFullTupleMap = new HashMap<Column,PrimitiveValue>(); 
		Tuple outTuple = new Tuple();
		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
		
		return outTuple;		
	}

   public ArrayList<Column> expressionAnalyzer(Expression expression){
		
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
    
    public int hashCodeCalculator(PrimitiveValue t) {
		
    	PrimitiveType dataType = t.getType();
    	int temp = 0;
    	switch(dataType){
		case BOOL:		
		    temp = t.hashCode();
		    break;
		case DATE:
			temp = t.hashCode();
			break;
		case DOUBLE:
			temp = t.hashCode();
			break;
		case LONG:
			temp = t.hashCode();				 
			break;
		case STRING:
			temp = t.hashCode();
			break;
		case TIME:
			temp = t.hashCode();
			break;
		default:			
			break;
		}			      
	
    	return temp;
    	
    }

	@Override
	public void print() {
		System.err.println("hashjoin 2table");
		
	}

}

