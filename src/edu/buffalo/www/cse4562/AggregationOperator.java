package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class AggregationOperator implements TupleIterator<Tuple>{
    
	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>(); // columns for group by
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C	
	boolean isOpen = true;
	
	//case1 function + no groupby
	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();// store all tuples of table
	
	//key -> hashcode, value -> ArrayList<Tuple>
	HashMap<Integer, ArrayList<Tuple>> hashCodeMap = new HashMap<>();
	
	boolean hasFunc = false;
	boolean hasGroupby = false;
	
	ArrayList<Integer> hashList = new ArrayList<>();// sotre key ---> hash code
	int keySize = 0;
	int mapCounter = 0;	
	
	int countercase1 = 0;
	
	public AggregationOperator(TupleIterator<Tuple> ti, Expression expHaving, List<Column> columnRefList, List<SelectItem> selectItems, Optimizer op) {
		this.ti = ti;
		this.expHaving = expHaving;
		this.columnRefList = columnRefList;
		this.selectItems = selectItems;
		this.hasFunc = op.hasFunc;
		this.hasGroupby = op.hasGroupby;
		this.open();
	}

	@Override
	public void open() {
		if(!isOpen) {
	    	   ti.open();
	    	   isOpen = true;
	       }		
	}

	@Override
	public void close() {
		if(isOpen) {
			ti.close();
			isOpen = false;
		}			
	}

	@Override
	public Tuple getNext() {
		
		HashMap<Column,PrimitiveValue> fullTupleMap = new  HashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple(fullTupleMap);	
		 
		//case1 there is group by in the query
		//eg coulumnRefList: R.A, R.B, T.C		
		//build hashcodeMap for group by
		if(columnRefList != null && !columnRefList.isEmpty()) {
			if(hashCodeMap.isEmpty()) {
				while(ti.hasNext()) {
				 tuple = ti.getNext();
					if(tuple != null) {	
						
						Evaluate evaluate = new Evaluate(tuple);					
						ArrayList<PrimitiveValue> tempValueList = new ArrayList<>();
						
						for(Column c : columnRefList) {											
							try {
								tempValueList.add(evaluate.eval(c));
							} catch (SQLException e) {
		                        System.out.println("group by eval can't find");
								e.printStackTrace();
							}						
						}
						
						Integer code = tempValueList.hashCode();
						//case1 new one: create new innertuplelist to store tuple add it to HashMap
						if(!hashCodeMap.containsKey(code)) {					
							ArrayList<Tuple> innertupleList = new ArrayList<>();
							innertupleList.add(tuple);
							hashCodeMap.put(code, innertupleList);
						}
						//case2 add to existing one: add new tuple to existing innertuplelist
						//put it into HasMap
						else {
							  ArrayList<Tuple> innertupleList = hashCodeMap.get(code);						
							  innertupleList.add(tuple);						
							  hashCodeMap.put(code, innertupleList);	
						}				
					}
				}	
			}
		}
		
		
		//Case2 no group by 
		//save the whole table into tupleList(memory)		
		if(columnRefList == null || columnRefList.isEmpty()) {
			if(tupleList.isEmpty()) {
			
				while(ti.hasNext()) {			 	
				    tuple = ti.getNext();//get tuple from projectOperator	
				    
					if(tuple == null) {
						this.close();
						break;
					}	
				
				    tupleList.add(tuple);			
			   }				
		
	        }
		}
		
		
		//projection		
		//tempTuple for 3 cases to use
		HashMap<Column,PrimitiveValue> fullTupleMaptemp = new HashMap<Column,PrimitiveValue>(); 
		Tuple tempTuple = new Tuple(fullTupleMaptemp);
		
		HashMap<Column,PrimitiveValue> fullTupleMaptemp3 = new HashMap<Column,PrimitiveValue>(); 
		Tuple outputTuple = new Tuple(fullTupleMaptemp3);	

		//case 1 has function + no groupby
		if(hasFunc && !hasGroupby) {
			
			int count = 0;
			countercase1 ++;
			
			for(SelectItem s: selectItems) {
				
				count ++;
							
				if(s instanceof SelectExpressionItem) {
					Expression expression = ((SelectExpressionItem) s).getExpression();				
					
					String alias = null;
					if(((SelectExpressionItem) s).getAlias() != null) {
						 alias = ((SelectExpressionItem) s).getAlias();//projection alias name	
					}										
					
					//parser function
					//Case1.3.1 function : a) count() b) max() c) min() d) avg() e)distinct()?
					if(expression instanceof Function) {
						Function function = (Function) expression;
						String functionName = function.getName();
					
						Aggregation aggregation = new Aggregation(function, functionName,tupleList);
											
						if(alias != null) {
							 outputTuple.setValue(alias,aggregation.getAggregation());
						}else {
							 String fakeAlias = functionName + "(" + count + ")";
							 outputTuple.setValue(fakeAlias,aggregation.getAggregation());
						}						
					}
												
				}
					
			}	
			
			return outputTuple;
		}
		
		// Don't need to be implemented
		//case 2 no fucntion + has groupby
		/*if(!hasFunc && hasGroupby) {
			
		}*/
				
		//case 3 has function + has groupby
		if(hasFunc && hasGroupby) {
			
			int count = 0;
			
			//load hashList
			if(hashList.isEmpty()) {
				Set<Integer> hashSet = hashCodeMap.keySet();
				hashList.addAll(hashSet);
				keySize = hashList.size();
			}
			
			//tempTupleList to store arraylist from hashcodemap
			ArrayList<Tuple> tempTupleList = new ArrayList<>();
			
			if(mapCounter < keySize) {				
			    tempTupleList = hashCodeMap.get(hashList.get(mapCounter));
				mapCounter ++;//last time mapCounter = keySize
	
			}else {
				return null;
			}
			
            //list<tuple> instead of single tuple
			if(!tempTupleList.isEmpty()) {
				tempTuple = tempTupleList.get(0); 
			}
			
			Evaluate evaluate = new Evaluate(tempTuple);
			
			for(SelectItem s: selectItems) {
				
				count ++;
				
				if(s instanceof SelectExpressionItem) {
					
					Expression expression = ((SelectExpressionItem) s).getExpression();									
					String alias = null;
					if(((SelectExpressionItem) s).getAlias() != null) {
						 alias = ((SelectExpressionItem) s).getAlias();//projection alias name	
					}					
					
					
					//parser function
					//Case1.3.1 function : a) count() b) max() c) min() d) avg() e)distinct()?
					if(expression instanceof Function) {
						Function function = (Function) expression;
						String functionName = function.getName();
					
						Aggregation aggregation = new Aggregation(function,functionName,tempTupleList);
											
						if(alias != null) {
							 outputTuple.setValue(alias,aggregation.getAggregation());
						}else {
							 String fakeAlias = functionName + "(" + count + ")";
							 outputTuple.setValue(fakeAlias,aggregation.getAggregation());
						}						
					}else {
						//Case1.3.2 expression				
						try {
						    if(expression == null) {
						    	return tuple;
						    }
						    else {				    		
							    	if(alias != null) {				    			 				    			 
							    	    outputTuple.setValue(((Column) expression).getTable(),alias,(PrimitiveValue)(evaluate).eval(expression));						    			
							    	}else {						    			 
							    	 	outputTuple.setValue(((Column) expression).getTable(),((Column) (expression)).getColumnName(),(PrimitiveValue)(evaluate).eval(expression));
							    	}
								 } 
						} catch (SQLException e) {
							e.printStackTrace();
							return null;
						}	
					}
	
				}
					
			}	
			
			return outputTuple;
		}

		return null;
	}

	@Override
	public boolean hasNext() {
		
		if(hasFunc && !hasGroupby) {
			if(countercase1 == 0) {
				return true;
			}
		}
		
		if(hasFunc && hasGroupby) {
			if(mapCounter == 0 || mapCounter < keySize ) {
				return true;
			}
		}

		return false;
	}
	
	


}
