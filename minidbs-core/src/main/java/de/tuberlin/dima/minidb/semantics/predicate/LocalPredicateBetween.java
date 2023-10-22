package de.tuberlin.dima.minidb.semantics.predicate;


import java.util.Map;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateConjunction;
import de.tuberlin.dima.minidb.semantics.Column;


/**
 * Internal representation of a between predicate.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class LocalPredicateBetween extends LocalPredicate
{
	/**
	 * The predicate for the lower bound.
	 */
	private final Predicate lowerBound;
	
	/**
	 * The predicate for the upper bound.
	 */
	private final Predicate upperBound;
	
	/**
	 * The literal marking the lower bound. 
	 */
	private final DataField lowerBoundLiteral;

	/**
	 * The literal marking the upper bound. 
	 */
	private final DataField upperBoundLiteral;
	
	/**
	 * The column on which the predicate is evaluated.
	 */
	private final Column column;
	
	
	/**
	 * Creates a new BETWEEN predicate.
	 * 
	 * @param column The column on which the predicate is evaluated.
	 * @param lowerBound The predicate for the lower bound.
	 * @param upperBound The predicate for the upper bound.
	 * @param lowerBoundLiteral The literal marking the lower bound. 
	 * @param upperBoundLiteral The literal marking the upper bound. 
	 */
	public LocalPredicateBetween(Column column, Predicate lowerBound, Predicate upperBound,
			DataField lowerBoundLiteral, DataField upperBoundLiteral)
	{
		this.column = column;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.lowerBoundLiteral = lowerBoundLiteral;
		this.upperBoundLiteral = upperBoundLiteral;
	}
	
	/**
	 * Gets the lowerBound from this OptimizerLocalPredicateBetween.
	 *
	 * @return The lowerBound.
	 */
	public Predicate getLowerBound()
	{
		return this.lowerBound;
	}

	/**
	 * Gets the upperBound from this OptimizerLocalPredicateBetween.
	 *
	 * @return The upperBound.
	 */
	public Predicate getUpperBound()
	{
		return this.upperBound;
	}

	/**
	 * Gets the lowerBoundLiteral from this LocalPredicateBetween.
	 *
	 * @return The lower-bound literal.
	 */
	public DataField getLowerBoundLiteral()
	{
		return this.lowerBoundLiteral;
	}
	
	/**
	 * Gets the upperBoundLiteral for this LocalPredicateBetween.
	 *
	 * @return The upper-bound literal.
	 */
	public DataField getUpperBoundLiteral()
	{
		return this.upperBoundLiteral;
	}
	
	/**
	 * Gets the column this predicate is evaluated on.
	 *
	 * @return The columnIndex.
	 */
	public Column getColumn()
	{
		return this.column;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(100);
		bld.append(this.lowerBoundLiteral.toString()).append(' ');
		bld.append(this.lowerBound.getOp().getSideSwitched().getSymbol()).append(' ');
		bld.append(this.lowerBound.getLeftHandColumn().getNodeContents()).append(' ');
		bld.append('(').append(this.column.getColumnIndex()).append(')');
		bld.append(' ').append(this.upperBound.getOp().getSymbol()).append(' ');
		bld.append(this.upperBoundLiteral.toString());
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createExecutablePredicate()
	 */
	@Override
	public de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate createExecutablePredicate()
	{
		de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate[] preds = {
				new LowLevelPredicate(this.lowerBound.getOp(), this.lowerBoundLiteral, this.column.getColumnIndex()),
				new LowLevelPredicate(this.upperBound.getOp(), this.upperBoundLiteral, this.column.getColumnIndex()),
		};
		return new LocalPredicateConjunction(preds);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createCopyAdjustedForProjectedTuple(java.util.Map)
	 */
	@Override
	public LocalPredicateBetween createCopyAdjustedForProjectedTuple(Map<Integer, Integer> columnMap)
	{
		Integer col = new Integer(this.column.getColumnIndex());
		Integer target = columnMap.get(col);
		if (target == null) {
			target = new Integer(columnMap.size());
			columnMap.put(col, target);
		}
		
		LocalPredicateBetween p = new LocalPredicateBetween(
				new Column(this.column.getRelation(), this.column.getDataType(), target.intValue()),
				this.lowerBound, this.upperBound, this.lowerBoundLiteral, this.upperBoundLiteral);
		p.selectivity = this.selectivity;
		p.truth = this.truth;
		return p;
	}

}
