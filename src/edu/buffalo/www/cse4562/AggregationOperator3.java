package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

public class AggregationOperator3 implements TupleIterator<Tuple>{
    
	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>(); // columns for group by
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C	
	boolean isOpen = true;
	

	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();// store all tuples of table		
	
	//key -> hashcode, value -> ArrayList<Tuple>
	HashMap<Integer, ArrayList<Object>> hashCodeMap = new HashMap<>();
	
	boolean hasFunc = false;
	boolean hasGroupby = false;
	
	ArrayList<Integer> hashList = new ArrayList<>();// sotre key ---> hash code
	int keySize = 0;
	int mapCounter = 0;	
	
	ArrayList<Object> objectList = new ArrayList<>();

	
	public AggregationOperator3(TupleIterator<Tuple> ti, Expression expHaving, List<Column> columnRefList, List<SelectItem> selectItems, Optimizer op) {
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
		
		LinkedHashMap<Column,PrimitiveValue> fullTupleMap = new  LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple tuple = new Tuple(fullTupleMap);
		
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
        
		Iterator<Tuple> it = tupleList.iterator();
		
		//case1 there is group by in the query
		//eg coulumnRefList: R.A, R.B, T.C		
		if(hashCodeMap.isEmpty()) {
			while(it.hasNext()) {
				tuple = it.next();
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
						
						ArrayList<Object> tempList = new ArrayList<>();						
						
						for(SelectItem s: selectItems) {

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
								
									Aggregation1 aggregation = new Aggregation1(function,functionName,alias);
									aggregation.getAggregation(tuple);					
									tempList.add(aggregation);					
								}
								//Case1.3.2 expression	
								else if(expression instanceof Column){
										Column column = (Column) expression;
										ColMix colmix = null;
										try {
											colmix = new ColMix(alias,evaluate.eval(column),column);
										} catch (SQLException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										tempList.add(colmix);
								} 
									
							}
				
						}
						hashCodeMap.put(code, tempList);		
				   }
						
				   //case2 add to existing one: add new tuple to existing innertuplelist
				   //put it into HasMap
				   else {
					    ArrayList<Object> t = hashCodeMap.get(code);
					    for(int i = 0; i < t.size(); i ++) {
					    	if(t.get(i) instanceof Aggregation1) {
					    		Aggregation1 a = ((Aggregation1) t.get(i));
					    		a.getAggregation(tuple);
					    		t.set(i, a);
					    	}
					    }
					    
					    hashCodeMap.put(code, t);
					  
				   }		
					
				}
			}
		}
		
		LinkedHashMap<Column,PrimitiveValue> fullTupleMaptemp3 = new LinkedHashMap<Column,PrimitiveValue>(); 
		Tuple outputTuple = new Tuple(fullTupleMaptemp3);
		
		//load hashList
		if(hashList.isEmpty()) {
			Set<Integer> hashSet = hashCodeMap.keySet();
			hashList.addAll(hashSet);
			keySize = hashList.size();
		}
		
		
		//output
		if(mapCounter < keySize) {				
		    objectList = hashCodeMap.get(hashList.get(mapCounter));
			mapCounter ++;//last time mapCounter = keySize

		}else {
			return null;
		}
		
		int j = 0;
		for(Object o : objectList) {
			 j ++;
			if(o instanceof ColMix) {
				if(((ColMix) o).alias == null) {
					outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).column.getColumnName(),((ColMix) o).data);
				}else {
					outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).alias,((ColMix) o).data);
				}
				
			}
			
			else if(o instanceof Aggregation1) {
				if(((Aggregation1) o).alias == null) {
					String fakealias = ((Aggregation1) o).funcName + j;
					outputTuple.setValue(fakealias,((Aggregation1) o).getAggregationValue(((Aggregation1) o).funcName));
				}else {
					outputTuple.setValue(((Aggregation1) o).alias,((Aggregation1) o).getAggregationValue(((Aggregation1) o).funcName));
				}
				
			}
		}
		
		
		return outputTuple;
		
	}

	@Override
	public boolean hasNext() {		

	   if(mapCounter == 0 || mapCounter < keySize ) {
			return true;
	   }		

		return false;
	}
	
	


}

