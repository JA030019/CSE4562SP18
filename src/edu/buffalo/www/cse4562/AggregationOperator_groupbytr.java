package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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

public class AggregationOperator_groupbytr implements TupleIterator<Tuple>{
    
	TupleIterator<Tuple> ti;
	Expression expHaving;
	List<Column> columnRefList = new ArrayList<Column>(); // columns for group by
	List<SelectItem> selectItems = new ArrayList<SelectItem>(); //SELECT A, B, C	
	boolean isOpen = true;
	
	//case1 function + no groupby
	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();// store all tuples of table
	
	//key -> hashcode, value -> ArrayList<Tuple>
	HashMap<ArrayList<PrimitiveValue>, ArrayList<Object>> hashCodeMap = new HashMap<>();
	
	boolean hasFunc = false;
	boolean hasGroupby = false;
	
	ArrayList<ArrayList<PrimitiveValue>> hashList = new ArrayList<>();// sotre key ---> hash code
	int keySize = 0;
	int mapCounter = 0;	
	
	ArrayList<Object> objectList = new ArrayList<>();

	
	public AggregationOperator_groupbytr(TupleIterator<Tuple> ti, Expression expHaving, List<Column> columnRefList, List<SelectItem> selectItems, Optimizer op) {
		this.ti = ti;
		this.expHaving = expHaving;
		this.columnRefList = columnRefList;
		this.selectItems = selectItems;
		this.hasFunc = op.hasFunc;
		this.hasGroupby = op.hasGroupby;
		this.open();
		this.print();
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
		 
		//case1 there is group by in the query
		//eg coulumnRefList: R.A, R.B, T.C		
		if(hashCodeMap.isEmpty()) {
			while(ti.hasNext()) {
				
			Tuple tuple = ti.getNext();
				if(tuple != null) {				
					ArrayList<PrimitiveValue> code = new ArrayList<>();
					
					for(Column c : columnRefList) {											
						code.add(tuple.fullTupleMap.get(c));				
					}

					//int code = code.hashCode();
					//case1 new one: create new innertuplelist to store tuple and add it to HashMap
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
								
									Aggregation2 aggregation = new Aggregation2(function,functionName,alias);
									aggregation.getAggregation(tuple);					
									tempList.add(aggregation);					
								}
								//Case1.3.2 expression	
								else if(expression instanceof Column){
										Column column = (Column) expression;
										ColMix colmix = null;										
										colmix = new ColMix(tuple.fullTupleMap.get(column),column);
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
					    	if(t.get(i) instanceof Aggregation2) {
					    		Aggregation2 a = ((Aggregation2) t.get(i));
					    		a.getAggregation(tuple);
					    		t.set(i, a);
					    	}
					    }					    
					    hashCodeMap.put(code, t);					  
				   }		
					
				}
				
			}
		}

		Tuple outputTuple = new Tuple();
		
		//load hashList
		if(hashList.isEmpty()) {
			Set<ArrayList<PrimitiveValue>> hashSet = hashCodeMap.keySet();
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
				/*if(((ColMix) o).alias == null) {
					outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).column.getColumnName(),((ColMix) o).data);
				}else {
					//outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).alias,((ColMix) o).data);
					//outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).alias,((ColMix) o).data);
				}*/
				outputTuple.setValue(((ColMix) o).column.getTable(),((ColMix) o).column.getColumnName(),((ColMix) o).data);
			}
			
			else if(o instanceof Aggregation2) {
				/*if(((Aggregation1) o).alias == null) {
					String fakeAlias = ((Aggregation1) o).funcName + j;
					outputTuple.setValue(fakeAlias,((Aggregation1) o).getAggregationValue(((Aggregation1) o).funcName));
				}else {
					outputTuple.setValue(((Aggregation1) o).alias,((Aggregation1) o).getAggregationValue(((Aggregation1) o).funcName));
				}*/
				outputTuple.setValue(((Aggregation2) o).alias,((Aggregation2) o).getAggregationValue(((Aggregation2) o).funcName));
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

	@Override
	public void print() {
		System.err.println("AggregationOperator2");
		
	}
	
}

