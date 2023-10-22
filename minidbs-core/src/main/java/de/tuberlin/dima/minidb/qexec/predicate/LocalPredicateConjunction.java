package de.tuberlin.dima.minidb.qexec.predicate;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.SerializationUtils;
import de.tuberlin.dima.minidb.qexec.QueryExecutionException;


/**
 * A predicate representing a conjunction of other local predicates.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class LocalPredicateConjunction implements LocalPredicate
{
	/**
	 * The predicates that are part of the conjunction.
	 */
	private LocalPredicate[] lps;

	/**
	 * Creates a predicate representing a conjunction over the given predicates.
	 * 
	 * @param lps The predicates that are part of the conjunction.
	 */
	public LocalPredicateConjunction(LocalPredicate[] lps)
	{
		this.lps = new LocalPredicate[lps.length];
		for (int i = 0; i < lps.length; i++) {
			if (lps[i] == null) {
				throw new NullPointerException("No null predicates allowed in conjunction.");
			}
			else {
				this.lps[i] = lps[i];
			}
		}
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate#evaluate(de.tuberlin.dima.minidb.core.DataTuple)
	 */
	@Override
	public boolean evaluate(DataTuple dataTuple) throws QueryExecutionException
	{
		for (int i = 0; i < this.lps.length; i++) {
			if (!this.lps[i].evaluate(dataTuple)) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Gets the factors of this conjunction.
	 * 
	 * @return The factors of this conjunction.
	 */
	public LocalPredicate[] getFactors()
	{
		return this.lps;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.lps.length; i++) {
			bld.append(this.lps[i]);
			if (i != this.lps.length - 1) {
				bld.append(" AND ");
			}
		}
		bld.append(")");
		return bld.toString();
	}

	/**
	 * Default constructor for serialization.
	 */
	public LocalPredicateConjunction() {};

	@Override
	public void readFields(DataInput in) throws IOException {
		lps = new LocalPredicate[in.readInt()];
		for (int i=0; i<lps.length; ++i) {
			lps[i] = SerializationUtils.readLocalPredicateFromStream(in);
		}
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(lps.length);
		for (int i=0; i<lps.length; ++i) {
			SerializationUtils.writeLocalPredicateToStream(lps[i], out);
		}
	}
}
