package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;

public class TableInfo2 {

	CreateTable ct;
	String tableName;
	
	ArrayList<String> pkColNameList = new ArrayList<>();
	ArrayList<String> fkColNameList = new ArrayList<>();
	
	/*//case1
	HashMap<PrimitiveValue, Integer> pkIndexMap = new HashMap<>();
	//case2
	
	//case3
	HashMap<String, HashMap<PrimitiveValue,ArrayList<Integer>>> wholefkIndexMap = new HashMap<>();*/
	
	Integer tableSize = 0;
	//bucket size
	int bucketSize = 100;         //to be modified
	
	
	public TableInfo2(String tableName, CreateTable ct) {
		this.ct = ct;
		this.tableName = tableName;
	}
	
	
	//collect key info to build index
	public void Parserkey(){
		
		/*//case1
		ArrayList<Index> indexList = (ArrayList<Index>) ct.getIndexes();
		
		if(indexList != null) {
			for(Index i: indexList) {
				ArrayList<String> pklist = (ArrayList<String>) i.getColumnsNames();
				pkColNameList.addAll(pklist);
			}
		}*/
		
		//case2
		ArrayList<ColumnDefinition> tempcd = (ArrayList<ColumnDefinition>) ct.getColumnDefinitions();
		for(ColumnDefinition cd : tempcd) {
			ArrayList<String> tempList = (ArrayList<String>) cd.getColumnSpecStrings();
			
			if(tempList != null) {
				if(tempList.contains("PRIMARY")) {
				pkColNameList.add(cd.getColumnName());
				}
				
				if(tempList.contains("REFERENCES")) {
					fkColNameList.add(cd.getColumnName());
				}
			}						
		}		
	}
	
	
	//build index
	public void indexBuilder() {

		
		//Table partition read original table and write on buckets in the disk
		//Update row# in the hashmap		
		
		//case1
		HashMap<PrimitiveValue, Integer> pkIndexMap = new HashMap<>();
		//case2
		
		//case3
		HashMap<String, HashMap<PrimitiveValue,ArrayList<Integer>>> wholefkIndexMap = new HashMap<>();
		
		BufferedReader reader;
		BufferedWriter writer = null;
		
		int countAll = 0;
		int bucketNumber = 0;
		//int bucketSize = 100;                                 //to be modified
		
		String readFilepath = "data/"+ tableName + ".dat";
		String writeFilepath = "data1/";                     //to be modified
		File file = new File(readFilepath);
		
        String line = null;
        
        //value for keys
        //case1
        PrimitiveValue temppk = null;
        /*//case2
        PrimitiveValue temppk1 = null;
        PrimitiveValue temppk2 = null;*/
        //case3
        PrimitiveValue tempfk1 = null;
        PrimitiveValue tempfk2 = null;
        PrimitiveValue tempfk3 = null;
        
        HashMap<PrimitiveValue,ArrayList<Integer>> fk1 = new HashMap<>();
        HashMap<PrimitiveValue,ArrayList<Integer>> fk2 = new HashMap<>();
        HashMap<PrimitiveValue,ArrayList<Integer>> fk3 = new HashMap<>();        
		
		try {
			reader = new BufferedReader(new FileReader(file));

			while((line = reader.readLine()) != null) {
				
				//System.out.println(countAll + " " +line);
				
				String[] columns = line.split("\\|");
				
				List<ColumnDefinition> columnDefinitions = ct.getColumnDefinitions();
				
				for(int i = 0; i< columns.length ;i++) {
					String columnName = columnDefinitions.get(i).getColumnName();
					
					//case1: single primary key
					if(pkColNameList.size() == 1) {
						if(columnName.equals(pkColNameList.get(0))) {
						  temppk = new LongValue(Long.parseLong(columns[i]));
					    }
					}
					
					/*//case2: double combined primary key
					if(pkColNameList.size() == 2) {
						
						if(columnName.equals(pkColNameList.get(0))) {
							temppk1 = new LongValue(Long.parseLong(columns[i]));  
					    }
						
						if(columnName.equals(pkColNameList.get(1))) {
							temppk2 = new LongValue(Long.parseLong(columns[i]));  
					    }
					}*/
					
					//case3: forign key
					if(!fkColNameList.isEmpty()) {
						
						//case3.1 single foreign key
						if(fkColNameList.size() == 1) {
							
							if(columnName.equals(fkColNameList.get(0))) {
								tempfk1 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk1.containsKey(tempfk1)) {
									ArrayList<Integer> fka1 = new ArrayList<>();							        
									fka1.add(countAll+1);
								    fk1.put(tempfk1, fka1);
								}else {
									ArrayList<Integer> templist = fk1.get(tempfk1);
								    templist.add(countAll+1);
								    fk1.put(tempfk1, templist);
								}
								
							}
							
						}
						
						//case3.2 double foregin keys
						if(fkColNameList.size() == 2) {
							
							if(columnName.equals(fkColNameList.get(0))) {
								tempfk1 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk1.containsKey(tempfk1)) {
									ArrayList<Integer> fka1 = new ArrayList<>();
									fka1.add(countAll+1);
								    fk1.put(tempfk1, fka1);
								}else {
									ArrayList<Integer> templist = fk1.get(tempfk1);
								    templist.add(countAll+1);
								    fk1.put(tempfk1, templist);
								}
							}
							
							if(columnName.equals(fkColNameList.get(1))) {
								tempfk2 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk2.containsKey(tempfk2)) {
									ArrayList<Integer> fka2 = new ArrayList<>();
									fka2.add(countAll+1);
								    fk2.put(tempfk2, fka2);
								}else {
									ArrayList<Integer> templist = fk2.get(tempfk2);
								    templist.add(countAll+1);
								    fk2.put(tempfk2, templist);
								}
							}
							
						}
						
						//case3.3 triple foregin keys
                        if(fkColNameList.size() == 3) {
                        	
                        	if(columnName.equals(fkColNameList.get(0))) {
								tempfk1 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk1.containsKey(tempfk1)) {
									ArrayList<Integer> fka1 = new ArrayList<>();
									fka1.add(countAll+1);
								    fk1.put(tempfk1, fka1);
								}else {
									ArrayList<Integer> templist = fk1.get(tempfk1);
								    templist.add(countAll+1);
								    fk1.put(tempfk1, templist);
								}
							}
                        	
                        	if(columnName.equals(fkColNameList.get(1))) {
								tempfk2 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk2.containsKey(tempfk2)) {
									ArrayList<Integer> fka2 = new ArrayList<>();
									fka2.add(countAll+1);
								    fk2.put(tempfk2, fka2);
								}else {
									ArrayList<Integer> templist = fk2.get(tempfk2);
								    templist.add(countAll+1);
								    fk2.put(tempfk2, templist);
								}
							}
                        	
                        	if(columnName.equals(fkColNameList.get(2))) {
								tempfk3 = new LongValue(Long.parseLong(columns[i]));
								
								if(!fk3.containsKey(tempfk3)) {
									ArrayList<Integer> fka3 = new ArrayList<>();
									fka3.add(countAll+1);
								    fk3.put(tempfk3, fka3);
								}else {
									ArrayList<Integer> templist = fk3.get(tempfk3);
								    templist.add(countAll+1);
								    fk3.put(tempfk3, templist);
								}
							}
                        	
						}
						
					}
					
				}				
				
				
				
				//update hashmap to build index				
				//hashmap of primary key
				
				
				//case1
				if(pkColNameList.size() == 1) {
					pkIndexMap.put(temppk, countAll+1);
				}
				
				
				//Ignore case 2 
				/*//case2
				if(pkColNameList.size() == 2) {
					
				}*/
				

				//get table size
				countAll ++;
				tableSize = countAll;
	
			}
			
			// wirte primary key on disk
			if(!pkIndexMap.isEmpty()) {
				//indexToDiskpk(pkIndexMap);
			}
			
			
			//update index hashmap for foreign key 
			//case3
			if(!fkColNameList.isEmpty()) {
				
			    //case3.1 single foreign key
				if(fkColNameList.size() == 1) {					
	            	
					//wholefkIndexMap.put(fkColNameList.get(0), fk1);
					indexToDiskfk(fk1,fkColNameList.get(0));
					
				}
				
				//case3.2 double foregin keys
	            if(fkColNameList.size() == 2) {           	            	
	            	
	            	/*wholefkIndexMap.put(fkColNameList.get(0), fk1);
	            	wholefkIndexMap.put(fkColNameList.get(1), fk2);	*/ 
	            	indexToDiskfk(fk2,fkColNameList.get(0));
	            	indexToDiskfk(fk1,fkColNameList.get(1));
				}
							
				//case3.3 triple foregin keys
	            if(fkColNameList.size() == 3) {	            	
	            	
	            	/*wholefkIndexMap.put(fkColNameList.get(0), fk1);
	            	wholefkIndexMap.put(fkColNameList.get(1), fk2);
	            	wholefkIndexMap.put(fkColNameList.get(2), fk3);*/
	            	
	            	indexToDiskfk(fk1,fkColNameList.get(0));
	            	indexToDiskfk(fk2,fkColNameList.get(1));
	            	indexToDiskfk(fk3,fkColNameList.get(2));
	            	
				}
			}
			

            
		} catch (Exception e) {			
			e.printStackTrace();
		}
		
		
	}
    
/*	//write index of primary key on disk
	public void indexToDiskpk(HashMap map) throws Exception {
		
		String filepath = "indexes/" + tableName + " " + pkColNameList.get(0) + ".dat";
		
		File f = new File(filepath);
		FileOutputStream fos = new FileOutputStream(f);
	    ObjectOutputStream oos = new ObjectOutputStream(fos);      
        
	    oos.writeObject(map);
	    oos.close();
	}*/
	
	//write index of foreign key on disk
	public void indexToDiskfk(HashMap map, String colName) throws Exception {
		
		//for(String str : fkColNameList) {
		    
			String filepath = "indexes/"+ tableName + " "+ colName + ".dat";
			
			File f = new File(filepath);
			FileOutputStream fos = new FileOutputStream(f);
		    ObjectOutputStream oos = new ObjectOutputStream(fos);      
	        
		    oos.writeObject(map);
		    oos.close();
		//}
		
	}
	
	
}

