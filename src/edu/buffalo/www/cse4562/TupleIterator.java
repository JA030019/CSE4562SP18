package edu.buffalo.www.cse4562;

public interface TupleIterator<Tuple> {

	public void open();
	public void close();
	public Tuple getNext();
    boolean hasNext();	
    public void print();	
   
}
