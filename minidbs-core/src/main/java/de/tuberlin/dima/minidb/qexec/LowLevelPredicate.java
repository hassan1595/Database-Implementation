package de.tuberlin.dima.minidb.qexec;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;


/**
 * The simples form of a local predicate, an atomic boolean condition of the
 * form &lt;column&gt; &lt;OPERATOR&gt; &lt;literal&gt;.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class LowLevelPredicate implements LocalPredicate
{	
	/**
	 * The literal in the condition of this predicate.
	 */
	private DataField literal;
	
	/**
	 * The index of the column used in the condition of this predicate. 
	 */
	private int colIndex;

	/**
	 * The constants used internally for the truth evaluation.
	 */
	private int c1, c2;
	
	/**
	 * Creates a new atomic boolean predicate representing the condition given
	 * by the operator and literal. The condition is evaluated on the column
	 * at the indicated position of the tuple.
	 * 
	 * @param predOp The operator (from the parse tree) used in the predicate.
	 * @param literal The literal value in the predicate. The instance must be of the
	 *                same type as the one in the relevant column of the
	 *                tuples to evaluate. 
	 * @param colIndex The position of the column that is used during evaluation.
	 */
	public LowLevelPredicate(Operator predOp, DataField literal, int colIndex)
	{
		this.literal = literal;
		this.colIndex = colIndex;
		
		// assign the constants depending on the predicate operator
		if (predOp.equals(Operator.EQUAL)) {
			this.c1 = this.c2 = 0;
		}
		else if (predOp.equals(Operator.GREATER)) {
			this.c1 = this.c2 = 1;
		}
		else if (predOp.equals(Operator.GREATER_OR_EQUAL)) {
			this.c1 = 0;
			this.c2 = 1;
		}
		else if (predOp.equals(Operator.NOT_EQUAL)) {
			this.c1 = -1;
			this.c2 = 1;
		}
		else if (predOp.equals(Operator.SMALLER)) {
			this.c1 = this.c2 = -1;
		}
		else if(predOp.equals(Operator.SMALLER_OR_EQUAL)) {
			this.c1 = -1;
			this.c2 = 0;
		}
		else {
			throw new IllegalArgumentException("Invalid operator.");
		}
	}
	
	/**
	 * Gets the index (= number) of the column that the predicate should be evaluated on.
	 * The number starts counting at 0;
	 * 
	 * @return The number of the column that the predicate applies to.
	 */
	public int getColumnIndex()
	{
		return this.colIndex;
	}
		
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple dataTuple) throws QueryExecutionException
	{
		return evaluateWithNull(dataTuple.getField(this.colIndex));
	}

	/**
	 * Evaluates the predicate against the given field. The field is compared to the literal
	 * with the logic of the operator that the predicate was instantiated with.
	 * <p>
	 * This method does not handle NULLs correctly, it should only be used when the column is
	 * known not to contain any NULL values.
	 * 
	 * @param field The field to evaluate the predicate against.
	 * 
	 * @return True, if the field passes the predicate, false otherwise.
	 */
	public boolean evaluate(DataField field)
	{
		int c = field.compareTo(this.literal);
		return (c == this.c1 | c == this.c2);
	}
	
	/**
	 * Evaluates the predicate against the given field. The field is compared to the literal
	 * with the logic of the operator that the predicate was instantiated with.
	 * <p>
	 * This method does handles NULLs correctly, under the assumption of monotonic logic, where
	 * all comparisons to NULL can be set to <i>false</i>, rather than <i>unknown</i>.
	 * 
	 * @param field The field to evaluate the predicate against.
	 * 
	 * @return True, if the field passes the predicate, false otherwise.
	 */
	public boolean evaluateWithNull(DataField field)
	{
		int c = field.compareTo(this.literal);
		boolean b = !field.isNULL();
		return (c == this.c1 | c == this.c2) & b;
	}
	
	/**
	 * Evaluates the predicate against the given fields. The fields are compared to the literal
	 * with the logic of the operator that the predicate was instantiated with. The results
	 * of the comparison are stored in the given boolean array.
	 * <p>
	 * This method does not handle NULLs correctly, it should only be used when the column is
	 * known not to contain any NULL values.
	 * 
	 * @param fields The field to evaluate the predicate against.
	 * @param result Output parameter indicating on which fields the predicate evaluated to True.
	 */
	public <T extends DataField> void evaluate(DataField[] fields, boolean[] result)
	{
		@SuppressWarnings("unchecked")
		T lt = (T) this.literal;
		int c1 = this.c1, c2 = this.c2;
		
		for (int i = 0; i < fields.length; i++) {
			@SuppressWarnings("unchecked")
			T ft = (T) fields[i];
			int c = ft.compareTo(lt);
			result[i] = (c == c1 | c == c2);
		}
	}
	
	/**
	 * Evaluates the predicate against the given fields. The fields are compared to the literal
	 * with the logic of the operator that the predicate was instantiated with. The results
	 * of the comparison are stored in the given boolean array.
	 * <p>
	 * This method does handles NULLs correctly, under the assumption of monotonic logic, where
	 * all comparisons to NULL can be set to <i>false</i>, rather than to <i>unknown</i>.
	 *
	 * @param fields The field to evaluate the predicate against.
	 * @param result Output parameter indicating on which fields the predicate evaluated to True.
	 */
	public <T extends DataField> void evaluateWithNull(DataField[] fields, boolean[] result)
	{
		@SuppressWarnings("unchecked")
		T lt = (T) this.literal;
		int c1 = this.c1, c2 = this.c2;
		
		for (int i = 0; i < fields.length; i++) {
			@SuppressWarnings("unchecked")
			T ft = (T) fields[i];
			int c = ft.compareTo(lt);
			boolean b = !ft.isNULL();
			result[i] = (c == c1 | c == c2) & b;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		String op;
		
		if (this.c1 == this.c2) {
			if (this.c1 == 0) {
				op = "=";
			}
			else if (this.c1 == 1) {
				op = ">";
			}
			else if (this.c1 == -1){
				op = "<";
			}
			else {
				op = "?";
			}
		}
		else {
			int x = this.c1 + this.c2;
			
			if (x == 1) {
				op = ">=";
			}
			else if (x == 0) {
				op = "<>";
			}
			else if (x == -1){
				op = "<=";
			}
			else {
				op = "?";
			}
		}
		
		return "col[" + this.colIndex + "] " + op + " " + this.literal;
	}

	/**
	 * Default constructor used for serialization.
	 */
	public LowLevelPredicate() {};
	
	/**
	 * Read constants from a stream.
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.literal = SerializationUtils.readDataFieldFromStream(in);
		this.colIndex = in.readInt();
		this.c1 = in.readInt();
		this.c2 = in.readInt();
	}

	/**
	 * Write constants to a stream.
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		SerializationUtils.writeDataFieldToStream(this.literal, out);
		out.writeInt(this.colIndex);
		out.writeInt(this.c1);
		out.writeInt(this.c2);
	}
}