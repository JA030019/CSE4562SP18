package edu.buffalo.www.cse4562;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.*;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/*
 * possible 5 cases for indexed nested loop join
 * 
 * 1. pk - fk or fk - pk(only under consideration so far)
 * 2. pk - pk
 * 3. fk - fk
 * 4. pk - null
 * 5. fk - null
*/

public class IndexJoinOperatorpf implements TupleIterator<Tuple>{
	
	TupleIterator<Tuple> tl;
	TupleIterator<Tuple> tr;
	Expression expression; // N.N_NATIONKEY = C.C_NATIONKEY only contains equal symbol && N_NATIONKEY --pk, C_NATIONKEY --fk
	boolean isOpen = true;
	
	ArrayList<String> tnList = new ArrayList<>();
	
	ArrayList<String> cnList = new ArrayList<>();
	
	TableInfo2 tableInfol;
	TableInfo2 tableInfor;
	
	String cnl = null;
	String cnr = null;
	
	HashMap<PrimitiveValue,ArrayList<Integer>> fkMap = new HashMap<>();
	HashMap<PrimitiveValue,Integer> pkMap = new HashMap<>();
		
	Tuple tempTupleL = new Tuple();
	Tuple tempTupleR = new Tuple();	
	
	Table tabler;
	String tnr;
	
	Column columnlpk;
	
	int listSize = 0;
	int counter = 0;
	
	TupleIterator<Tuple> trr = null;
	Optimizer op;
	
	ArrayList<Integer> rowList = new ArrayList<>();

	
	public IndexJoinOperatorpf(TupleIterator<Tuple> tl, TupleIterator<Tuple> tr, Expression expression,Table tabler) throws Exception, Exception {
		
		this.tl = tl;
        this.tr = tr;
        this.tabler = tabler;
        
        this.tnr = tabler.getName(); //to be modified
        
        this.expression = expression;
        this.cnList = getColumnNames(expression);
        this.tnList = getTableNames(expression);
        
        this.tableInfol = Main.fullIndexMap.get(tnList.get(0));
        this.tableInfor = Main.fullIndexMap.get(tnList.get(1));
        
        if(!tnList.get(1).equals(tnr)) {
        	tnList = reverseList(tnList);
        	cnList = reverseList(cnList);
        	this.tableInfol = Main.fullIndexMap.get(tnList.get(0));
            this.tableInfor = Main.fullIndexMap.get(tnList.get(1));
        }
        
        this.columnlpk = new Column(new Table(tnList.get(0)), cnList.get(0));

        this.op = Main.optimizerMap.get(tabler.getName());
        this.open();
        this.print();
        
        //pk
        if(tableInfol.pkColNameList.contains(cnList.get(0))) {
        	cnl = "primarykey";
			System.err.println(cnList.get(0) + " is primary Key");
		}		 
		if(tableInfol.fkColNameList.contains(cnList.get(0))) {
			cnl = "foreignkey";
			readFromDisk(cnl,tnList.get(0),cnList.get(0));
			System.err.println(cnList.get(0) + " is foreign Key");
		}		
				 
		//fk
		if(tableInfor.fkColNameList.contains(cnList.get(1))) {
			cnr = "foreignkey";
			
			readFromDisk(cnr,tnList.get(1),cnList.get(1));
			System.err.println(cnList.get(1) + " is foreign Key");
		} 
		
		if(tableInfor.pkColNameList.contains(cnList.get(1))) {
			cnr = "primarykey";
			
			System.err.println(cnList.get(1) + " is primary Key");
		} 
		
		
		if(cnl.equals("foreignkey") && cnr.equals("primarykey")) {
			columnlpk = new Column(new Table(tnList.get(1)), cnList.get(1));
			this.tabler = new Table("LINEITEM");
		}
			
	}
	

	@Override
	public void open() {
		if(!isOpen) {
			tl.open();
			//tr.open();
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
		
		//case A fk - pk
		if(cnl.equals("foreignkey") && cnr.equals("primarykey")) {
			
			//inialize left tuple
			if(tempTupleL.fullTupleMap.isEmpty()) {
				tempTupleL = tr.getNext();
			}
			
			while(!fkMap.containsKey(tempTupleL.fullTupleMap.get(columnlpk))) {
				tempTupleL = tr.getNext();
				if(tempTupleL == null) {
	            	return null;
				}	
			}

			if(fkMap.containsKey(tempTupleL.fullTupleMap.get(columnlpk))) {
				
				//update rowlist for tupel from right side
				if(rowList.isEmpty()){
					rowList = fkMap.get(tempTupleL.fullTupleMap.get(columnlpk)); 
					listSize = rowList.size();
					
					//create new readrowoperator
					ReadRowOperator ri = new ReadRowOperator(tabler,rowList);
					
					//Checkpoint4 always projection pushdown					
					ProjectOperator pi = new ProjectOperator(ri,op.optProMap.get(tabler.getName()));					
					
					//selection push down to be modified
					if(!op.selectelements.containsKey(tabler.getName())) {
						trr = pi;
						
					}else {
						SelectOperator si = new SelectOperator(pi,op.selectelements.get(tabler.getName()));
					
					    trr = si;
					  
					}
					
					
				}else {

					// get tuple in the postion of row# in the table
					if(trr != null){
						tempTupleR = trr.getNext();
					}
					
					
					if(tempTupleR != null) {
						tupleCombine = joinTuple(tempTupleR, tempTupleL);
					    return tupleCombine;
					}
					
					//update tempTupleL
					if(tempTupleR == null) {
						tempTupleL = tr.getNext();
						rowList.clear();
						if(tempTupleL == null) {
			            	return null;
			            }
											
						return this.getNext();
					}
					
				}
				
				// get tuple in the postion of row# in the table
				if(trr != null) {
					tempTupleR = trr.getNext();
				}
				
				
				if(tempTupleR != null) {
					tupleCombine = joinTuple(tempTupleR, tempTupleL);
				    return tupleCombine;
				}
				
				//update tempTupleL
				if(tempTupleR == null) {
					tempTupleL = tr.getNext();
					rowList.clear();
					if(tempTupleL == null) {
		            	return null;
		            }
										
					return this.getNext();
				}
				
			}

			
		}
		
		//case B pk - fk
		if(cnl.equals("primarykey") && cnr.equals("foreignkey")) {
			
			//inialize left tuple
			if(tempTupleL.fullTupleMap.isEmpty()) {
				tempTupleL = tl.getNext();
			}
			
			while(!fkMap.containsKey(tempTupleL.fullTupleMap.get(columnlpk))) {
				tempTupleL = tl.getNext();
				if(tempTupleL == null) {
	            	return null;
				}	
			}

			if(fkMap.containsKey(tempTupleL.fullTupleMap.get(columnlpk))) {
				
				//update rowlist for tupel from right side
				if(rowList.isEmpty()) {
					rowList = fkMap.get(tempTupleL.fullTupleMap.get(columnlpk)); 
					listSize = rowList.size();
					
					//create new readrowoperator
					ReadRowOperator ri = new ReadRowOperator(tabler,rowList);
					
					//Checkpoint4 always projection pushdown
					
					ProjectOperator pi = new ProjectOperator(ri,op.optProMap.get(tabler.getName()));
					//System.out.println(pi==null);
					
					//selection push down to be modified
					if(!op.selectelements.containsKey(tabler.getName())) {
						trr = pi;
						//System.out.println(trr==null);
					}else {
						SelectOperator si = new SelectOperator(pi,op.selectelements.get(tabler.getName()));
					
					    trr = si;
					    //System.out.println(trr==null);
					}
					
					
				}else {

					// get tuple in the postion of row# in the table
					if(trr != null){
						tempTupleR = trr.getNext();
					}
					
					
					if(tempTupleR != null) {
						tupleCombine = joinTuple(tempTupleL, tempTupleR);
					    return tupleCombine;
					}
					
					//update tempTupleL
					if(tempTupleR == null) {
						tempTupleL = tl.getNext();
						rowList.clear();
						if(tempTupleL == null) {
			            	return null;
			            }
											
						return this.getNext();
					}
					
				}
				
				// get tuple in the postion of row# in the table
				if(trr != null) {
					tempTupleR = trr.getNext();
				}
				
				
				if(tempTupleR != null) {
					tupleCombine = joinTuple(tempTupleL, tempTupleR);
				    return tupleCombine;
				}
				
				//update tempTupleL
				if(tempTupleR == null) {
					tempTupleL = tl.getNext();
					rowList.clear();
					if(tempTupleL == null) {
		            	return null;
		            }
										
					return this.getNext();
				}
				
			}

		}

		return null;
	}

	@Override
	public boolean hasNext() {
		
		//if(cnl.equals("primarykey") && cnr.equals("foreignkey")) {
			
			if(tempTupleL != null){
				return true;
			}
			
			/*if(!rowList.isEmpty()) {
				
			}*/
			
			return false;
			
		//}
		
		//return false;
	}

	@Override
	public void print() {
		System.err.println("index nested loop join");
		
	}
	
	
	public Tuple joinTuple(Tuple t1, Tuple t2){

		Tuple outTuple = new Tuple();
		outTuple.fullTupleMap.putAll(t1.fullTupleMap);
		outTuple.fullTupleMap.putAll(t2.fullTupleMap);
		
		return outTuple;		
	}
	
	public ArrayList<String> getColumnNames(Expression exp){
		
		ArrayList<String> cnList = new ArrayList<>();
		
		Expression l = ((BinaryExpression) expression).getLeftExpression();
		Expression r = ((BinaryExpression) expression).getRightExpression();
				
		if(l instanceof Column) {
			Column columnl = (Column)l;
			cnList.add(columnl.getColumnName());
		}
		
		if(r instanceof Column) {
			Column columnr = (Column)r;
			cnList.add(columnr.getColumnName());
		}	
		
		return cnList;
		
	}
	
    public ArrayList<String> getTableNames(Expression exp){
		
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
    
    public void readFromDisk(String keytype,String tableName, String columnName) throws IOException, Exception {
	    
        String filepath = "indexes/" + tableName + " " + columnName + ".dat";
		
		File f = new File(filepath);
    	
	    FileInputStream inputStream = new FileInputStream(f);
	    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
	    
	    if(keytype.equals("primarykey")) {
	    	
	    	pkMap = (HashMap<PrimitiveValue, Integer>) objectInputStream.readObject();
	    	
	    	//test show
	    	//System.err.println("Primarykey");
		    /*Set<PrimitiveValue> s = pkMap.keySet();
		    for(PrimitiveValue p : s) {
		    	
		    	System.err.println(p + " " + pkMap.get(p));
		    }*/
	    }
	    
	    if(keytype.equals("foreignkey")) {
	    	fkMap = (HashMap<PrimitiveValue, ArrayList<Integer>>) objectInputStream.readObject();
	    }
	   
	    
    }
    
    
    public ArrayList<String> reverseList(ArrayList<String>tnList){
    	
    	ArrayList<String> newtnList = new ArrayList<>();
    	
    	int i;
    	for(i= tnList.size()-1; i>-1; i--) {
    		newtnList.add(tnList.get(i));
    	}
    	
		return newtnList;
    	
    }
	

}
