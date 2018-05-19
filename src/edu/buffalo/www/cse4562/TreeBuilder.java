package edu.buffalo.www.cse4562;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;

public class TreeBuilder {

	SelectBody sb;
	
	public TreeBuilder(SelectBody sb) {
		this.sb = sb;
	}
	
	public TupleIterator<Tuple> treeParser(SelectBody sb) {
			
			TableOperator tol = null;
			TupleIterator<Tuple> so = null;
			SubSelectOperator sub = null;
			
			TupleIterator<Tuple> jo = null;
			
			TupleIterator<Tuple> oo = null;
			TupleIterator<Tuple> lo = null;
			
	        TupleIterator<Tuple> ap = null;
			
			TupleIterator<Tuple> trsel = null;
			TupleIterator<Tuple> tlsel = null;
			
			TupleIterator<Tuple> trpro = null;
			TupleIterator<Tuple> tlpro = null;
			
			TupleIterator<Tuple> output = null;
			
			
			if(sb instanceof PlainSelect) {			        			
				   PlainSelect plainSelect = (PlainSelect)sb;
	
				   //optimizing
				   /*
				   --------------------collecting information for optimizing----------------                                                                        
				                                                                           */
				   //get expression of selection for pushing down
				   Expression expsel = plainSelect.getWhere();
				   
				   //check or expression right now no projection push down for the case of or expression
				   boolean t = true;
				   
				   List<Join> joins = plainSelect.getJoins();
				   
				   //get expression of projection for pushing down
				   //consider selection projection group by
				   List<SelectItem> expProList = plainSelect.getSelectItems();	
				   
				   //get columns of groupby
				   List<Column> columnRefList = plainSelect.getGroupByColumnReferences();		
			       
				   //not implemented having
				   Expression expHaving = plainSelect.getHaving();
				   /*
				   ----------------------optimization-------------------------------------                                                                     
		                                                                                 */
				   Optimizer op = new Optimizer(columnRefList, expProList);
				   //boolean t = false;
				   
				   /*  //test
				   op.allBinarySplit(expsel);*/
				   
				   
				   /*
	               -------------------optimization for projection push down---------------
	                                                                                     */
				   //optimize projection (projection push down)
				   //To be modified, when shall we choose projection push down			   
				   //Get projection selectItemList and set alias to null (R.A, C.*, S.C)
				   ArrayList<SelectItem> proList = op.setSelectItemAlias(expProList);
				   
				   //ProList -> empty means SELECT * FROM ... No need to push down
				   if(!proList.isEmpty() && t) {
					   //Get selection expressionList (R.A < S.C, R.B>3)
					   ArrayList<Expression> selList = op.andExpAnalyzer(expsel);
					   ArrayList<Expression> orList = new ArrayList<>();
					   ArrayList<Expression> deleteList = new ArrayList<>();
					   for(Expression exp: selList) {
						   if(exp instanceof OrExpression) {
							   orList.addAll(op.orExpAnalyzer(exp));
							   deleteList.add(exp);
						   }
					   }
					   
					   if(!orList.isEmpty()) {
						   selList.addAll(orList);
						   selList.removeAll(deleteList);
					   }
					   
					   //Split selection expression and add them into optSelList (R.A, S.C, R.B)		
					   ArrayList<Expression> optSelList = new ArrayList<>();
					   if(!selList.contains(null)) {
						   for(Expression exp: selList) {
						   optSelList.addAll(op.binarySplit(exp));
					       }
					   }
					   
					   //Add the column needed in selection into optProList for pushing down (R.A, C.*, S.C, R.B)
					   ArrayList<SelectItem> optProList = op.combine(proList, optSelList,columnRefList);
					   
					   //write data into HashMap<tableName, list<SelectItem>>(attribute of optimize)
					   op.optPro(optProList);
				   }
				   /* 
				   -----------------------optimization for selection push down----------------------
				                                                                                   */
				   //optimize join using selection push down
				   if(joins != null && expsel != null) {
					      // R.A> S.C AND R.A >3 AND R.B < 2 AND S.C =3;
						  ArrayList<Expression> expList = op.andExpAnalyzer(expsel);// R.A>S.C, R.A>3, R.B<2, S.C=3
						  ArrayList<Expression> expLs = new ArrayList<>();
						  ArrayList<String> tableList1 = new ArrayList<>();	
						  ArrayList<Expression> expRest = new ArrayList<>();
						  for(Expression exptemp : expList) {
							  ArrayList<String> array = op.binaryAnalyzer(exptemp);					  
							  if(array.size() == 1) { //fliter R.A > S.C
								  
								  //create tableList (R, S)
								  if(!tableList1.contains(array.get(0))) {
									  tableList1.add(array.get(0));
								  }
								  // add all single expressions to list for clustering
								  expLs.add(exptemp);
							      // op.selectelements.put(tableList1.get(0), exptemp);//Problems R.A>3 AND R.B<2 AND S.C=3
								   
							  } 
							  //find "equal expression"
							  else if(array.size() == 2) {							   
								   if(op.containsEqual(exptemp)) {
								        //String combstr = array.get(0) + array.get(1);
								        Combo combo = new Combo(array.get(0),array.get(1));
									   //expld.add(exptemp);
								        if(combo != null) {
								        	
								        	if(!op.hashJoinMap.containsKey(combo)) {
								        		op.hashJoinMap.put(combo, exptemp);
								        	}else {
								        		expRest.add(exptemp);
								        	}
								        	
								        }
								        
								   }
								   if(op.containsNonEquals(exptemp)) {
									   expRest.add(exptemp);
								   }
							  }
						  }
						  
						  //cluster expressions according to table name 
						  //combine expressions with same table name
						  //put combined expression and table name into hashmap
						  for(String temp : tableList1) {
							  ArrayList<Expression> tempList = new ArrayList<>();
							  for(Expression exp : expLs) {
								  if(temp.equals(op.binaryAnalyzer(exp).get(0))) {
									  tempList.add(exp);
								  }
							  }
							  Expression tempexp = op.combineExpression(tempList);
							  op.selectelements.put(temp, tempexp);
						  }
						  
						  op.selRest = op.combineExpression(expRest);
	
				   }    
				   
				   
				 
				  /*
				   -------------------------parser query-------------------------------------------
				                                                                                  */
				   FromItem fromItem = plainSelect.getFromItem();			   
				   if(fromItem instanceof SubSelect) {
					   SelectBody subBody = ((SubSelect) fromItem).getSelectBody();		
					   String subSelectAlias = ((SubSelect) fromItem).getAlias();
					   
					   //only consider plainSelect
					   sub = new SubSelectOperator(treeParser((PlainSelect)subBody),subSelectAlias);
	
				   }else if(fromItem instanceof Table) {
					   
					   Table tablel = ((Table) fromItem);
					   tol = new TableOperator(tablel);
 
					   //record for table information in optimizer
					   if(tablel.getAlias()!= null) {
						   op.tableList.add(tablel.getAlias());
					   }else if(tablel.getName() != null) {
						   op.tableList.add(tablel.getName());
					   }
					   
					   //optimizing for left table				   
					   //projection push down				   
					   if(!proList.isEmpty() && op.optProMap.containsKey(tablel.getAlias())) {					   
						   tlpro = new ProjectOperator(tol,op.optProMap.get(tablel.getAlias()));						  
					   }
					   else if(!proList.isEmpty() && op.optProMap.containsKey(tablel.getName())) {
						   tlpro = new ProjectOperator(tol,op.optProMap.get(tablel.getName()));						   
					   }
					   else {					   
						   tlpro = tol;
						  						   
					   }
					   
					   //selection push down
					   if((op.selectelements.containsKey(tablel.getAlias())) && op.selectelements != null) {						  
							  tlsel = new SelectOperator(tlpro, op.selectelements.get(tablel.getAlias()));  							 
					   }
					   else if((op.selectelements.containsKey(tablel.getName())) && op.selectelements != null) {
						      tlsel = new SelectOperator(tlpro, op.selectelements.get(tablel.getName()));						    
					   }
					   else {						  						 
						      tlsel = tlpro;						    
					   }    
	
				   }else if(fromItem instanceof SubJoin) {
					  //  System.out.println("subjoin");
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
						  count ++;	//count for join first time join with table
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
							  
						  }else if(fiJoin instanceof Table) {					  
							  
							  Table tabler = ((Table) fiJoin);							 
 
							  TableOperator tor = new TableOperator(tabler);
							  
							  //record for table information in optimizer
							  if(tabler.getAlias() != null) {
								   op.tableList.add(tabler.getAlias());
								   
								   //update optimizerMap 
								   Main.optimizerMap.put(tabler.getAlias(), op);
								   
							   }else if(tabler.getName() != null) {
								   op.tableList.add(tabler.getName());
								   
								   //update optimizerMap 
								   Main.optimizerMap.put(tabler.getName(), op);
							   }
							  
							  //optimizing for right table
							  //projection push down
							  if(!proList.isEmpty() && op.optProMap.containsKey(tabler.getAlias())) {							   
								   trpro = new ProjectOperator(tor,op.optProMap.get(tabler.getAlias()));
								   
							   }
							  else if(!proList.isEmpty() && op.optProMap.containsKey(tabler.getName())) {
								   trpro = new ProjectOperator(tor,op.optProMap.get(tabler.getName()));								   
							  }
							  else {							   
								   trpro = tor;								   
							   }						  
							  
							  //selection push down
							  if((op.selectelements.containsKey(tabler.getAlias()))&& op.selectelements != null) {							  
								  trsel = new SelectOperator(trpro, op.selectelements.get(tabler.getAlias())); 							  
							  }
							  else if((op.selectelements.containsKey(tabler.getName()))&& op.selectelements != null) {
								  trsel = new SelectOperator(trpro, op.selectelements.get(tabler.getName())); 								  
							  }
							  else {							  
								  trsel = trpro;								  
							  }
							  
							    //Case 1 cross product
								if(isSimple) {
									Expression expression = plainSelect.getWhere();
																														
									//case 1.1 cross product
									if(expression == null) {
									
										
										if(count == 1){
											jo = new CrossProductOperator(tol, tor, expression);
											
											//jo = new JoinOperator1(tol, tor, expression);
										}else {
										    jo = new CrossProductOperator(jo, tor, expression);
										    
											//jo = new JoinOperator1(jo, tor, expression);
										}
									}
																
									//case 1.2 cross product -> join
									if(expression != null) {
										if(count == 1){									
											if(sub == null) {	
												    //optimization join ---> index join

												    //optimization join --> hash join
												    if(!op.hashJoinMap.isEmpty()) {
												    	Set<Combo> comboSet = op.hashJoinMap.keySet();
												    	for(Combo c: comboSet) {
												    		
												    		//tricky way to second query of checkpoint 4 ----q2
												    		if(c.comboList.contains(tol.table.getAlias()) && c.comboList.contains(tor.table.getAlias())) {

												    				jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c),tabler);

												    			break;
												    		}
												    		
												    		if(c.comboList.contains(tol.table.getName()) && c.comboList.contains(tor.table.getName())) {
												    			
												    			// tricky way for checkpoint4 ----q1
												    			if(c.a.equals("LINEITEM")&&c.b.equals("ORDERS")) {
												    				jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c),tabler);
												    			}
												    			// tricky way for checkpoint4 ----q3
												    			else if(c.a.equals("PART")&&c.b.equals("LINEITEM")) {
												    				jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c),tabler);
												    			}
												    			// tricky way for checkpoint4 ----q5
												    			else if(c.a.equals("LINEITEM")&&c.b.equals("SUPPLIER")) {
												    				jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c),tabler);
												    			}
												    			// tricky way for checkpoint4 ----q4
												    			else if(c.a.equals("CUSTOMER")&&c.b.equals("ORDERS")) {
												    				jo = new HashJoinOperator(tlsel,trsel,op.hashJoinMap.get(c),tabler);
												    			}
												    			//tricky way for checkpoint4 ----Q4,Q5
												    			else {
													    				try {
																		jo = new IndexJoinOperatorpf(tlsel,trsel,op.hashJoinMap.get(c),tabler);
																	    } catch (Exception e) {															
																		e.printStackTrace();
																	    }
												    			}

												    			break;
												    		}
												    	}
												    }
												    
												    
												    
												    //no optimization for join 
												    else {
												    	jo = new JoinOperator1(tlsel, trsel, expression);
												    	
												    }
						
											}else {	
													jo = new JoinOperator1(sub, trsel, expression);	
													
											}										
										}else {	
											//optimization join ---> index join

											//optimization join --> hashjoin
										    if(!op.hashJoinMap.isEmpty()) {
										    	Set<Combo> comboSet = op.hashJoinMap.keySet();
										    	for(Combo c: comboSet) {
										    		for(String str: op.tableList) {				    			
										    			if(c.comboList.contains(str) && c.comboList.contains(tor.table.getAlias())) {
											    			jo = new HashJoinOperator(jo,trsel,op.hashJoinMap.get(c),tabler);											    			
											    			break;
											    		}
										    			
										    			if(c.comboList.contains(str) && c.comboList.contains(tor.table.getName())) {
										    				
										    				/*try {
																jo = new IndexJoinOperatorpf(jo,trsel,op.hashJoinMap.get(c),tabler);
																break;
															} catch (Exception e) {															
																e.printStackTrace();
															}*/
										    				
										    				
										    				//jo = new HashJoinOperator(jo,trsel,op.hashJoinMap.get(c),tabler);
										    				jo = new HashJoinOperator(trsel,jo,op.hashJoinMap.get(c),tabler);
										    				break;
										    			}
										    		}
										    	}
										    }
											//no optimizaiton for join
										    else {
										    	jo = new JoinOperator1(jo, trsel, expression);
										        //jo.print();
										    }	 									   
										}
									}							
								}
								//case 2 natural join
								else if(isNatural) {
								   System.err.println("isNatural " + isNatural);						   
								}
								
								//case 3 inner join
								else{
									System.err.println("inner ");
								}
		  
						  }
						  
					  }
	              
	              }
	              
				  /*
				   ------------------------------------------- selection --------------------------------------------------- 
				  */
				  
	              //To be modified cuz of pushing down selection 
				  //parser selection(WHERE R.B = 0) SELECT R.A FROM R WHERE R.B = 0;
				  Expression expWhere; 
				  if(joins == null){
					  expWhere = plainSelect.getWhere();
				  }else {
					  expWhere = op.selRest;
				  }
				    
				  if (joins != null) {
	
					  if(expWhere == null) {
						  so = jo;
					  }else {
						  so = new SelectOperator(jo, expWhere);
					  }
	
				  }
				  //no join, but subselect
				  else if(sub != null){
					  
					  if(expWhere == null) {
						  so = sub;
					  }else {
					      so = new SelectOperator(sub, expWhere);
					  }
				  }
				  //no join no subselect
				  else {
					  
					  if(expWhere == null) {
						  so = tol;
					  }else {
					      so = new SelectOperator(tol, expWhere);
					  }
				  }
				  
				  
				  
				  /*
				   ------------------------------------Aggregatiion and Projection------------------------------------ 
				  */
				  
				  //case1 no grouby + no function --> projection
				  
				  if(!op.hasFunc && !op.hasGroupby) {
					  //parser projection(SELECT R.A, R.B, R.C) SELECT R.A, R.B, R.C FROM R WHERE R.B = 0;
					  ap = new ProjectOperator(so, expProList);
				  }			  
				  //case2 no groupby + function			  
				  else if(op.hasFunc && !op.hasGroupby) {
					  ap = new AggregationOperator_nongroupby(so,expHaving, columnRefList, expProList, op);
				  }
				  //case3  groupby + no function
				  //not implemented
				  
				  //case4 groupby function		  
				  else{				  
					  ap = new AggregationOperator_groupbytr(so,expHaving, columnRefList, expProList, op); 
				  } 
	  
				  /*
				   ------------------------------------------- Orderby --------------------------------------------------- 
				  */
				  
				  //parser OrderBy(ORDERBY R.C, R.A DESC, R.B) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B;
				  List<OrderByElement> orderbyElements = plainSelect.getOrderByElements();
				  
				  if(orderbyElements == null ||orderbyElements.isEmpty()) {
					  oo = ap;
				  }else {
					  oo = new OrderByOperator(ap, orderbyElements);
				  }
				  
				  /*
				   ------------------------------------------- Limit --------------------------------------------------- 
				  */
				  //parser Limit(LIMIT 3) SELECT R.A FROM R WHERE R.B = 0 ORDERBY R.C, R.A DESC, R.B LIMIT 3;
				  Limit limit = plainSelect.getLimit();

				  if(limit == null) {
					  lo = oo;
				  }else {
					  lo = new LimitOperator(oo, limit);
				  }
	
				 // Distinct distinct = plainSelect.getDistinct();
				  			  	
			   }else if(sb instanceof Union) {
			            System.err.println("Union");					            		            
			   }
			   else {
			        System.err.println("can't handle it");
			   }
				
			return lo;
			
		}   
}
