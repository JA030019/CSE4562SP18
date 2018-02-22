package edu.buffalo.www.cse4562;

import java.sql.SQLException;
import java.util.Hashtable;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class Evaluate extends Eval  {

	//Expression expression;
	Tuple tuple;
	
	public Evaluate (Tuple tuple) {
		//this.expression = expression;
		this.tuple = tuple;
	}
	

	@Override
	public PrimitiveValue eval(Column c) throws SQLException {
		 return tuple.getTupleData(c.getColumnName().toLowerCase());
	}	
	
}
