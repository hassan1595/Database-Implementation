package de.tuberlin.dima.minidb.qexec.predicate;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * The simplest form of a join predicate, a  boolean condition of the
 * form &lt;tuple1.columnA&gt; &lt;OPERATOR&gt; &lt;tuple2.columnB&gt;.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class JoinPredicateAtom implements JoinPredicate
{	
	/**
	 * The index of the column from the left hand side tuple 
	 * used in the condition of this predicate. 
	 */
	private int leftHandTupleColIndex;
	
	/**
	 * The index of the column from the right hand side tuple 
	 * used in the condition of this predicate. 
	 */
	private int rightHandTupleColIndex;
	
	/**
	 * The constants used internally for the truth evaluation.
	 */
	private int c1, c2;
	
	/**
	 * Creates a new atomic join predicate representing the condition between two tuples
	 * as given by the operator.
	 * The condition is evaluated on the columns at the indicated positions of the tuples.
	 * 
	 * @param predOp The operator (from the parse tree) used in the predicate.
	 * @param leftHandColumnIndex The position of the left hand tuple's column that is used
	 *                            during evaluation.
	 * @param rightHandColumnIndex The position of the right hand tuple's column that is used
	 *                            during evaluation.
	 */
	public JoinPredicateAtom(Operator predOp, int leftHandColumnIndex, int rightHandColumnIndex)
	{		
		this.leftHandTupleColIndex = leftHandColumnIndex;
		this.rightHandTupleColIndex = rightHandColumnIndex;
		
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
		
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple leftHandSide, DataTuple rightHandSide) throws QueryExecutionException
	{
		final DataField f1 = leftHandSide.getField(this.leftHandTupleColIndex);
		final DataField f2 = rightHandSide.getField(this.rightHandTupleColIndex);
		
		final int c = f1.compareTo(f2);
		final boolean b1 = !f1.isNULL();
		final boolean b2 = !f2.isNULL();
		
		return (c == this.c1 | c == this.c2) & b1 & b2;
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
		
		return "left.col[" + this.leftHandTupleColIndex + "] " + op + " right.col[" + this.rightHandTupleColIndex + "]";
	}

	/**
	 * Default constructor for serialization.
	 */
	public JoinPredicateAtom() {}

	@Override
	public void readFields(DataInput in) throws IOException {
		leftHandTupleColIndex = in.readInt();
		rightHandTupleColIndex = in.readInt();
		c1 = in.readInt();
		c2 = in.readInt();
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(leftHandTupleColIndex);
		out.writeInt(rightHandTupleColIndex);
		out.writeInt(c1);
		out.writeInt(c2);
	}


	@Override
	public boolean isEquiJoin() {
		return this.c1 == this.c2 && this.c1 == 0;
	}
	
	public int getLeftJoinColumn() {
		return leftHandTupleColIndex;
	}
	
	public int getRightJoinColumn() {
		return rightHandTupleColIndex;
	}
}
