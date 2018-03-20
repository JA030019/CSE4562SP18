package edu.buffalo.www.cse4562;

import java.util.*;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVParser;

import java.io.Reader;
import java.io.InputStreamReader;
import net.sf.jsqlparser.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main {
   	
	static String prompt = "$> "; // expected prompt
	public static HashMap<String,CreateTable> tableMap = new HashMap<>();
	
	public static void main(String[] argsArray) throws Exception{			
		
		
		System.out.println(prompt);
		System.out.flush();
		
		Reader in = new InputStreamReader(System.in);		
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement statement;
		
		//long startTime=System.currentTimeMillis(); //long endTime=System.
        while((statement = parser.Statement()) != null){     
        	
        	long startTime=System.currentTimeMillis(); //long endTime=System.
        	
		     if(statement instanceof CreateTable) {				        		
			      CreateTable ct = new CreateTable();
			      ct = (CreateTable)statement;			      
			      tableMap.put(ct.getTable().toString().toLowerCase(), ct);			      			    			      
			      
			 }else if(statement instanceof Select) {
				       SelectBody sb = ((Select) statement).getSelectBody();				        		
				       
				       TupleIterator<Tuple> t = treeParser(sb);
				       
				       while(t.hasNext()) {
		        			try {
		        				Tuple tuple = t.getNext();		        				
		        				if(tuple != null) {
		        					tuple.printTuple();
		        				}											
							} catch (Exception  e) {
								e.printStackTrace();
							}
	        			}
			 }          		
		     // read for next query
             System.out.println(prompt);
             System.out.flush();
             long endTime = System.currentTimeMillis(); 
             System.out.println("Time = " + (endTime -startTime)); 	
        }
	}
	
	public static TupleIterator<Tuple> treeParser(SelectBody sb) {
		
		TableOperator tol = null;
		SelectOperator so = null;
		SubSelectOperator sub = null;
		
		JoinOperator jo = null;
		ProjectOperator po = null;
		OrderByOperator oo = null;
		LimitOperator lo = null;
		
		TupleIterator<Tuple> tr = null;
		TupleIterator<Tuple> tl = null;
		
		if(sb instanceof PlainSelect) {			        			
			   PlainSelect plainSelect = (PlainSelect)sb;
			  
			   
			   //optimizing
			   Expression exp = plainSelect.getWhere();
			   List<Join> joins = plainSelect.getJoins();		   
			   Optimizer op = new Optimizer(); // for sigle column expression			  
			   
			   if(joins != null && exp != null) {				 
					  ArrayList<Expression> expList = op.exprssionAnalyzer(exp);
					  ArrayList<Expression> expld = new ArrayList<>();
					  ArrayList<String> tableList1 = null;					  
					  for(Expression exptemp : expList) {
						  if(op.binaryAnalyzer(exptemp).size() == 1) {
							   tableList1 = op.binaryAnalyzer(exptemp);  					 
						       op.optelements.put(tableList1.get(0), exptemp);
						  } 
						  else if(op.binaryAnalyzer(exptemp).size() == 2) {
							   expld.add(exptemp);
						  }
					  }
					  
					  //Combine double expression
					  if(!expld.isEmpty()) {
						  
						  //expld.get(0)
						  Expression opex = null;
						  for(Expression extemp : expld) {
							  opex = new AndExpression(opex, extemp);   
						  }
						  						  
						  opex = ((BinaryExpression) opex).getRightExpression();
						  op.adex = opex;
                   }
					  
				  }    
			  //    Boolean b = (!op.optelements.isEmpty()) && (op.optelements != null);
			   
			   FromItem fromItem = plainSelect.getFromItem();			   
			   if(fromItem instanceof SubSelect) {
				   SelectBody subBody = ((SubSelect) fromItem).getSelectBody();		
				   String subSelectAlias = ((SubSelect) fromItem).getAlias();
				   
				   //only consider plainSelect
				   sub = new SubSelectOperator(treeParser((PlainSelect)subBody),subSelectAlias);

			   }else if(fromItem instanceof Table) {
				   
				   Table tablel = ((Table) fromItem);
				   tol = new TableOperator(tablel);
				   
				   //optimizing for left table
				   if((op.optelements.containsKey(tablel.getAlias())) && op.optelements != null) {
						  
						  tl = new SelectOperator(tol, op.optelements.get(tablel.getAlias()));  
				   }else {
						  
						 // tl = new SelectOperator(tol,null);
					      tl = tol;
				   }    
				   
				  // System.out.println("left table "+tablel.getName());
			   }else if(fromItem instanceof SubJoin) {
				    System.out.println("subjoin");
			   }
			  
			  //parser Join
			  //SELECT * FROM R, S, T WHERE R.A = S.C, S.D = T.F;
			  //List<Join> joins = plainSelect.getJoins();
			  
			  if(joins != null) {
				  boolean isNatural = false;
				  boolean isInner = false;
				  boolean isSimple = false;
			      //count for join(eg: S, T)
				  int count = 0;

				  for(Join join : joins) {
					  count ++;	
					  isSimple = join.isSimple();
					  isNatural = join.isInner();
					  isInner = join.isInner();
					  
					  FromItem fiJoin = join.getRightItem();
					  
					  if(fiJoin instanceof SubSelect) {
						  /*SelectBody subBody = ((SubSelect) fiJoin).getSelectBody();		
						  String subSelectAlias = ((SubSelect) fiJoin).getAlias();
						   
						  //only consider plainSelect
						  sub = new SubSelectOperator(treeParser((PlainSelect)subBody),subSelectAlias);*/
	
					  }else if(fiJoin instanceof SubJoin) {
						  //not consider for now
						  
					  }else if(fiJoin instanceof Table) {					  
						  
						  Table tabler = ((Table) fiJoin);

						  
						  //System.out.println("right table" + tabler.getName());
						  TableOperator tor = new TableOperator(tabler);
						  
						  //optimizing for right table
						  if((op.optelements.containsKey(tabler.getAlias()))&& op.optelements != null) {
							  
							  tr = new SelectOperator(tor, op.optelements.get(tabler.getAlias()));  
						  }else {
							  
							  //tr = new SelectOperator(tor,null);
							  tr = tor;
						  }
						  

						    //Case 1 cross product
							if(isSimple) {
								Expression expression = plainSelect.getWhere();
							//	System.out.println("cross product " + isSimple);													
								
								//case 1.1 cross product
								if(expression == null) {
								//	System.out.println("cross product");
									
									if(count == 1){
										
										jo = new JoinOperator(tol, tor, expression);
									}else {
									    jo = new JoinOperator(jo, tor, expression);
									}
								}
															
								//case 1.2 cross product -> join
								if(expression != null) {
									//System.out.println("check if we can turn it into join");								
									//Boolean b = (!op.optelements.isEmpty()) && (op.optelements != null);
									if(count == 1){									
										if(sub == null) {
											
										//	if(b) {//optimize
										//		jo = new JoinOperator(tl, tr, op.adex); 
										//	}else {// no optimize
												jo = new JoinOperator(tl, tr, expression);
										//	}
											
										}else {
											
										//	if(b) {
										//		jo = new JoinOperator(sub, tr, op.adex);
										//	}else {
												jo = new JoinOperator(sub, tr, expression);
										//	}
											
										}										
									}else {
										
									//	if(b) {
									//		 jo = new JoinOperator(jo, tr, op.adex);
									//	}else {
											 jo = new JoinOperator(jo, tr, expression);
									//	}
									   
									}
								}							
							}
							//case 2 natural join
							else if(isNatural) {
							   System.out.println("isNatural " + isNatural);						   
							}
							
							//case 3 inner join
							else{
								System.out.println("inner ");
							}
	  
					  }
					  
				  }
              
              }
  
              //To be modified cuz of pushing down selection 
			  //paser selection(WHERE R.B = 0) SELECT R.A FROM R WHERE R.B = 0;
			  Expression expWhere = plainSelect.getWhere();
			  
			  //?????????
			  if (joins != null) {
				//  if(b) {
				//	  so =  new SelectOperator(jo, null);
				//  }else {
					  so = new SelectOperator(jo, expWhere);
				//  }
				  
			  }
			  //no join, but subselect
			  else if(sub != null){
				  so = new SelectOperator(sub, expWhere);
			  }
			  //no join no subselect
			  else {
				  so = new SelectOperator(tol, expWhere);
			  }
			  
              
			  //To be modified cuz of pushing down projection in future
			  //paser projection(SELECT R.A, R.B, R.C) SELECT R.A, R.B, R.C FROM R WHERE R.B = 0;
			  List<SelectItem> selectItems = plainSelect.getSelectItems();
			  po = new ProjectOperator(so, selectItems);
			  
			  //paser OrderBy(ORDERBY R.C, R.A DESC, R.B) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B;
			  List<OrderByElement> orderbyElements = plainSelect.getOrderByElements();
			  oo = new OrderByOperator(po, orderbyElements);
			  
			  //paser Limit(LIMIT 3) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B LIMIT 3;
			  Limit limit = plainSelect.getLimit();
			  lo = new LimitOperator(oo, limit);
			  
             /* //For checkpoint 3
			  Expression expHaving = plainSelect.getHaving();
			  List<Column> columnRef = plainSelect.getGroupByColumnReferences();
			  
			  Distinct distinct = plainSelect.getDistinct();*/
			  			  	
		   }else if(sb instanceof Union) {
		            System.out.println("Union");					            		            
		   }
		   else {
		        System.out.println("can't handle it");
		   }
			
		return lo;
		
	}   
	
	
    
}


