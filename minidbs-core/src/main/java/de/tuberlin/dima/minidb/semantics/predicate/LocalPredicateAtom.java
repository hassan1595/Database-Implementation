package de.tuberlin.dima.minidb.semantics.predicate;


import java.util.Map;

import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.semantics.Column;


/**
 * The representation of a predicate, consisting of a single condition of the type
 * <i>column-expression &lt;OP&gt; literal</i>.
 * <p>
 * The LocalPredicateAtom instances contain a description of the column contained in the predicate,
 * plus the original parsed predicate. If the predicate is part of a column-expression, the
 * expression can be found as a part of the parsed predicate.
 * <p>
 * Furthermore, the instance contains the literal against which the column(-expression) is compared. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class LocalPredicateAtom extends LocalPredicate
{
	/**
	 * A reference to the original parsed predicate.
	 */
	private final Predicate parsedPredicate;
	
	/**
	 * The column involved in the local predicate.
	 */
	private final Column column;
	
	/**
	 * The literal against which this local predicate atom compares.
	 */
	private final DataField literal;

	
	
	/**
	 * Creates an optimizer local predicate for the given parsed predicate.
	 * The schema information and the column index allows this predicate to
	 * be turned into an executable predicate.
	 * 
	 * If the predicate is on a CHAR column, this constructor pads the literal
	 * with blanks in order to match the column content (which is of fix length
	 * and consequently padded with blanks).  
	 * 
	 * @param parsedPredicate The predicate from the parser.
	 * @param column The referenced column.
	 * @param literalValue The literal against which the predicate will be evaluated.
	 */
	public LocalPredicateAtom(Predicate parsedPredicate, Column column, DataField literalValue)
	{
		super();
		
		this.column = column;
		this.parsedPredicate = parsedPredicate;
		this.literal = literalValue;
	}
	
	
	/**
	 * Gets the parsedPredicate from this LocalPredicateAtom.
	 *
	 * @return The parsedPredicate.
	 */
	public Predicate getParsedPredicate()
	{
		return this.parsedPredicate;
	}

	/**
	 * Gets the literal from this LocalPredicateAtom.
	 *
	 * @return The literal.
	 */
	public DataField getLiteral()
	{
		return this.literal;
	}

	/**
	 * Gets the column from this local predicate is evaluated on.
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
		
		String col = this.parsedPredicate.getLeftHandColumn() != null ? 
				this.parsedPredicate.getLeftHandColumn().toString() :
				this.parsedPredicate.getLeftHandAliasColumn().getResultColumnName();
		
		bld.append(col).append(' ');
		bld.append('(').append(this.column.getColumnIndex()).append(')').append(' ');
		bld.append(this.parsedPredicate.getOp().getSymbol());
		bld.append(' ').append(this.literal.toString());
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createExecutablePredicate()
	 */
	@Override
	public LowLevelPredicate createExecutablePredicate()
	{
		return new LowLevelPredicate(this.parsedPredicate.getOp(), this.literal, this.column.getColumnIndex());
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.predicates.OptimizerLocalPredicate#createCopyAdjustedForProjectedTuple(java.util.Map)
	 */
	@Override
	public LocalPredicateAtom createCopyAdjustedForProjectedTuple(Map<Integer, Integer> columnMap)
	{
		Integer col = new Integer(this.column.getColumnIndex());
		Integer target = columnMap.get(col);
		if (target == null) {
			// column not yet contained
			target = new Integer(columnMap.size());
			columnMap.put(col, target);
		}
		
		LocalPredicateAtom p = new LocalPredicateAtom(this.parsedPredicate, new Column(this.column.getRelation(), this.column.getDataType(), target.intValue()), this.literal);
		p.selectivity = this.selectivity;
		p.truth = this.truth;
		
		return p;
	}
}
