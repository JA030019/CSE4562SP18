package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class HashJoinOperator implements TupleIterator<Tuple>{

	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression; // L.A = R.C only contains equal symbol
	boolean isOpen = true;
	
	ArrayList<Tuple> tupleListR = new ArrayList<Tuple>();// store all tuples of right table

	ArrayList<Tuple> tupleHashList = new ArrayList<Tuple>();
	int count = 0;// flag the position of tuple in the tupleHashList
	
	HashMap<Integer, ArrayList<Tuple>> hashcodeMap = new HashMap<>();

	Tuple tempTupleL = new Tuple();

	int hashcodeL = 0;	

	Tuple tempTupleR = new Tuple();		

	Tuple temp = new Tuple();
	
	Column targetColumn = null;
	
	// table information from tableinfo
	ArrayList<String> tnList = new ArrayList<>();	
	
	Table tabler;
	String tnr;
	ArrayList<Column> columnList = new ArrayList<Column>();
	
	public HashJoinOperator(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression, Table tabler) {
	
		this.tl = tl;
        this.tr = tr;
        this.tnList = getTableAlias(expression);
        this.tabler = tabler;
        this.tnr = tabler.getAlias();
        
        if(!tnList.get(1).equals(tnr)) {
        	this.expression = reverseExpression(expression);        	
        }else {
        	this.expression = expression;
        }
        this.columnList = expressionAnalyzer(this.expression);
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
			isOpen = false;
		}	
		
	}

	@Override
	public Tuple getNext() {

		while(true) {
		
		Tuple tupleCombine = null;		

		//write in right tuple into tuplelist
		//calculate hashcode for targeted column and build hashcodemap<hashcode, list<tuple>>
		if(tupleListR.isEmpty()) {
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
					//int hashCode = hashCodeCalculator(tuple.fullTupleMap.get(targetColumn));
					int hashCode = tuple.fullTupleMap.get(targetColumn).hashCode();
					if(!hashcodeMap.containsKey(hashCode)) {
						ArrayList<Tuple> atmp = new ArrayList<Tuple>();
						atmp.add(tuple);
						hashcodeMap.put(hashCode, atmp);
						
					}else {
						//ArrayList<Tuple> atmp = new ArrayList<Tuple>();
						ArrayList<Tuple> atmp = hashcodeMap.get(hashCode);
						atmp.add(tuple);
						hashcodeMap.put(hashCode, atmp);						
					}
				}
			}
		}		
		
		columnList.remove(targetColumn);
		//inialize left tuple and get corresponding hashcode (first time)		
		if(tempTupleL.fullTupleMap.isEmpty()) {
			tempTupleL = tl.getNext();
			if(tempTupleL != null) {

				hashcodeL = tempTupleL.fullTupleMap.get(columnList.get(0)).hashCode();		

			}
		}
		
		//HashJoin
		while(!hashcodeMap.containsKey(hashcodeL)) {
            tempTupleL = tl.getNext();
            if(tempTupleL == null) {
            	return null;
            }           

		    hashcodeL = tempTupleL.fullTupleMap.get(columnList.get(0)).hashCode();		
		
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
					
					//update tuple from left table
					tempTupleL = tl.getNext();
					if(tempTupleL == null) {
		            	return null;
		            }
					
					count = 0;

				    hashcodeL = tempTupleL.fullTupleMap.get(columnList.get(0)).hashCode();		

					//return this.getNext();
				}
			}
		}
		
		}
		//return null;
	}

	@Override
	public boolean hasNext() {
		
		if(tl.hasNext()) {
			return true;
		}else {
			tl.close();
		}
		
		if(tempTupleL == null) {
			return false;
		}
		
		//tl.hasNext() -> false
		if(count < tupleHashList.size()) {
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

	@Override
	public void print() {
		System.err.println("hashjoin 1 table" + tnList);		
	}
	
	//modified for checkpoint 4
    public ArrayList<String> getTableAlias(Expression expression){
		
		ArrayList<String> tnList = new ArrayList<>();
		
		Expression l = ((BinaryExpression) expression).getLeftExpression();
		Expression r = ((BinaryExpression) expression).getRightExpression();
				
		if(l instanceof Column) {
			
			//use tricky way to fix bugs of no alias
			if(((Column) l).getTable().getName() == null) {
				String tableName = null;

    			String na = ((Column) l).getColumnName().split("_")[0];
    			if(na.equals("P")) {
    				tableName = "PART";    				 
    				tnList.add(tableName);
    			}else if(na.equals("S")) {
    				tableName = "SUPPLIER";
    				tnList.add(tableName);
    			}else if(na.equals("PS")) {
    				tableName = "PARTSUPP";
    				tnList.add(tableName);
    			}else if(na.equals("C")) {
    				tableName = "CUSTOMER";
    				tnList.add(tableName);
    			}else if(na.equals("O")) {
    				tableName = "ORDERS";
    				tnList.add(tableName);
    			}else if(na.equals("L")) {
    				tableName = "LINEITEM";
    				tnList.add(tableName);
    			}else if(na.equals("N")) {
    				tableName = "NATION";
    				tnList.add(tableName);
    			}else if(na.equals("R")) {
    				tableName = "REGION";
    				tnList.add(tableName);
    			}
			}else {
				Column columnl = (Column)l;
			    tnList.add(columnl.getTable().getName());
			}			
			
		}
		
		if(r instanceof Column) {
			
			//use tricky way to fix bugs of no alias
			if(((Column) r).getTable().getName() == null) {
				String tableName = null;

    			String na = ((Column) r).getColumnName().split("_")[0];
    			if(na.equals("P")) {
    				tableName = "PART";    				 
    				tnList.add(tableName);
    			}else if(na.equals("S")) {
    				tableName = "SUPPLIER";
    				tnList.add(tableName);
    			}else if(na.equals("PS")) {
    				tableName = "PARTSUPP";
    				tnList.add(tableName);
    			}else if(na.equals("C")) {
    				tableName = "CUSTOMER";
    				tnList.add(tableName);
    			}else if(na.equals("O")) {
    				tableName = "ORDERS";
    				tnList.add(tableName);
    			}else if(na.equals("L")) {
    				tableName = "LINEITEM";
    				tnList.add(tableName);
    			}else if(na.equals("N")) {
    				tableName = "NATION";
    				tnList.add(tableName);
    			}else if(na.equals("R")) {
    				tableName = "REGION";
    				tnList.add(tableName);
    			}
			}else {
				Column columnr = (Column)r;
			    tnList.add(columnr.getTable().getName());
			}
		}					
		return tnList;		
	}
    

    
    public Expression reverseExpression(Expression exp) {
    	
    	Expression temp = null;
	
    	if(exp instanceof BinaryExpression) {
			Expression l = ((BinaryExpression) exp).getLeftExpression();
			Expression r = ((BinaryExpression) exp).getRightExpression();
			
			temp = new EqualsTo(r,l);
    	}	
	
		return temp;
    	
    }

}
