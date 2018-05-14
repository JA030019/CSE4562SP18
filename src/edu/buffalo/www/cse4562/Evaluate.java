package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class Evaluate extends Eval  {

	Tuple tuple;
	
	public Evaluate (Tuple tuple) {		
		this.tuple = tuple;
	}	

	@Override
	public PrimitiveValue eval(Column c) {

		 return tuple.fullTupleMap.get(c);
	}
	
	
}
