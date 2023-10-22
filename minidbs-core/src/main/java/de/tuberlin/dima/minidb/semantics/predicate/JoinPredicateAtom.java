package de.tuberlin.dima.minidb.semantics.predicate;


import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * The internal representation of an atomic join predicate.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class JoinPredicateAtom implements JoinPredicate, Comparable<JoinPredicateAtom>
{
	/**
	 * The predicate that was originally parsed.
	 */
	private final Predicate parsedPredicate;
	
	/**
	 * The relation producing the left hand join column.
	 */
	private final Relation leftHandRelation;
	
	/**
	 * The left hand join column in its relation.
	 */
	private final Column leftHandColumn;
	
	/**
	 * The table producing the right hand join column.
	 */
	private final Relation rightHandRelation;
	
	/**
	 * The right hand join column in its table.
	 */
	private final Column rightHandColumn;
	
	
	/**
	 * Creates a new join predicate atom.
	 * 
	 * @param parsedPredicate The predicate that was originally parsed.
	 * @param leftHandTable The table producing the left hand join column.
	 * @param rightHandTable The table producing the right hand join column.
	 * @param leftHandColumn The left hand join column.
	 * @param rightHandColumn The right hand join column.
	 */
	public JoinPredicateAtom(Predicate parsedPredicate, Relation leftHandTable,
			Relation rightHandTable, Column leftHandColumn, Column rightHandColumn)
	{
		this.parsedPredicate = parsedPredicate;
		this.leftHandRelation = leftHandTable;
		this.rightHandRelation = rightHandTable;
		this.leftHandColumn = leftHandColumn;
		this.rightHandColumn = rightHandColumn;
	}
	
	/**
	 * Gets the parsedPredicate from this OptimizerJoinPredicateAtom.
	 *
	 * @return The parsedPredicate.
	 */
	public Predicate getParsedPredicate()
	{
		return this.parsedPredicate;
	}

	/**
	 * Gets the leftHandOriginatingTable from this OptimizerJoinPredicateAtom.
	 *
	 * @return The leftHandOriginatingTable.
	 */
	public Relation getLeftHandOriginatingTable()
	{
		return this.leftHandRelation;
	}

	/**
	 * Gets the left-hand column from this JoinPredicateAtom.
	 *
	 * @return The left-hand column.
	 */
	public Column getLeftHandColumn()
	{
		return this.leftHandColumn;
	}

	/**
	 * Gets the rightHandOriginatingTable from this OptimizerJoinPredicateAtom.
	 *
	 * @return The rightHandOriginatingTable.
	 */
	public Relation getRightHandOriginatingTable()
	{
		return this.rightHandRelation;
	}

	/**
	 * Gets the right-hand column from this JoinPredicateAtom.
	 *
	 * @return The right-hand column.
	 */
	public Column getRightHandColumn()
	{
		return this.rightHandColumn;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a copy of this predicate atom where the sides are switched, i.e. the left
	 * hand side becomes the right hand side and vice versa.
	 * 
	 * @return The side switched copy.
	 */
	@Override
	public JoinPredicateAtom createSideSwitchedCopy()
	{
		Predicate oldPred = this.getParsedPredicate();
		Predicate pp = new Predicate();
		pp.setLeftHandSide(oldPred.getRightHandExpression(), oldPred.getRightHandColumn());
		pp.setRightHandSide(oldPred.getLeftHandExpression(), oldPred.getLeftHandColumn());
		pp.setOperator(oldPred.getOp().getSideSwitched());
		
		JoinPredicateAtom na = new JoinPredicateAtom(pp,
				this.getRightHandOriginatingTable(), 
				this.getLeftHandOriginatingTable(),
				this.getRightHandColumn(),
				this.getLeftHandColumn());
		
		return na;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerJoinPredicate#createExecutablepredicate()
	 */
	@Override
	public de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom createExecutablepredicate()
	{
		return new de.tuberlin.dima.minidb.qexec.predicate.JoinPredicateAtom(
				this.parsedPredicate.getOp(), 
				this.leftHandColumn.getColumnIndex(), this.rightHandColumn.getColumnIndex());
	}
	
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((this.leftHandColumn == null) ? 0 : this.leftHandColumn.hashCode());
		result = prime
				* result
				+ ((this.leftHandRelation == null) ? 0 : this.leftHandRelation.hashCode());
		result = prime * result
				+ ((this.parsedPredicate == null) ? 0 : this.parsedPredicate.hashCode());
		result = prime * result
				+ ((this.rightHandColumn == null) ? 0 : this.rightHandColumn.hashCode());
		result = prime
				* result
				+ ((this.rightHandRelation == null) ? 0 : this.rightHandRelation
						.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JoinPredicateAtom other = (JoinPredicateAtom) obj;
		if (this.leftHandColumn == null) {
			if (other.leftHandColumn != null)
				return false;
		} else if (!this.leftHandColumn.equals(other.leftHandColumn))
			return false;
		if (this.leftHandRelation == null) {
			if (other.leftHandRelation != null)
				return false;
		} else if (!this.leftHandRelation.equals(other.leftHandRelation))
			return false;
		if (this.parsedPredicate == null) {
			if (other.parsedPredicate != null)
				return false;
		} else if (!this.parsedPredicate.equals(other.parsedPredicate))
			return false;
		if (this.rightHandColumn == null) {
			if (other.rightHandColumn != null)
				return false;
		} else if (!this.rightHandColumn.equals(other.rightHandColumn))
			return false;
		if (this.rightHandRelation == null) {
			if (other.rightHandRelation != null)
				return false;
		} else if (!this.rightHandRelation.equals(other.rightHandRelation))
			return false;
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(100);
		
		bld.append(this.parsedPredicate.getLeftHandColumn().getNodeContents()).append(' ');
		bld.append('(').append(this.leftHandRelation).append('.');
		bld.append(this.leftHandColumn.getColumnIndex()).append(')').append(' ');
		bld.append(this.parsedPredicate.getOp().getSymbol());
		bld.append(' ').append(this.parsedPredicate.getRightHandColumn().getNodeContents()).append(' ');
		bld.append('(').append(this.rightHandRelation).append('.');
		bld.append(this.rightHandColumn.getColumnIndex()).append(')');
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(JoinPredicateAtom o) {
		return this.toString().compareTo(o.toString());
	}
}
