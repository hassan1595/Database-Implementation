package de.tuberlin.dima.minidb.qexec.predicate;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * A predicate representing a disjunction of join predicates.
 * 
 * Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class JoinPredicateDisjunction implements JoinPredicate
{
	/**
	 * The predicates that are part of the disjunction.
	 */
	private JoinPredicate[] predicates;

	/**
	 * Creates a predicate representing a disjunction over the given predicates.
	 * 
	 * @param ps The predicates that are part of the disjunction.
	 */
	public JoinPredicateDisjunction(JoinPredicate[] ps)
	{
		this.predicates = new JoinPredicate[ps.length];
		for (int i = 0; i < ps.length; i++) {
			if (ps[i] == null) {
				throw new NullPointerException("No null predicates allowed in disjunction.");
			}
			else {
				this.predicates[i] = ps[i];
			}
		}
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple, de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple leftHandSide, DataTuple rightHandSide) throws QueryExecutionException 
	{
		for (int i = 0; i < this.predicates.length; i++) {
			if (this.predicates[i].evaluate(leftHandSide, rightHandSide)) {
				return true;
			}
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.predicates.length; i++) {
			bld.append(this.predicates[i]);
			if (i != this.predicates.length - 1) {
				bld.append(" OR ");
			}
		}
		bld.append(")");
		return bld.toString();
	}
	
	/**
	 * Default constructor for serialization.
	 */
	public JoinPredicateDisjunction() {}

	@Override
	public void readFields(DataInput in) throws IOException {
		predicates = new JoinPredicate[in.readInt()];
		for (int i=0; i<predicates.length; ++i) {
			predicates[i] = SerializationUtils.readJoinPredicateFromStream(in);
 		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(predicates.length);
		for (int i=0; i<predicates.length; ++i) {
			SerializationUtils.writeJoinPredicateToStream(predicates[i], out);
		}
	}
	
	@Override
	public boolean isEquiJoin() {
		return false;
	}
}
