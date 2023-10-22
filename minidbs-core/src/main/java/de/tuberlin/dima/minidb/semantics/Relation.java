package de.tuberlin.dima.minidb.semantics;


import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;


/**
 * 
 * The internal representation of a relation, which was in a FROM clause.
 * May be an actual table or the result of a nested-table-expression.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class Relation extends OptimizerPlanOperator
{
	/**
	 * The cardinality of this relation after its access and local predicates.
	 */
	protected long cardinality = -1;
	
	/**
	 * The ID for this relation. The ID may be set to define an order over the
	 * relations to simplify analytic algorithms. 
	 */
	private int ID;
	
	//-------------------------------------------------------------------------

	/**
	 * Sets the ID for this relation.
	 * 
	 * @param id The id.
	 */
	public void setID(int id)
	{
		this.ID = id;
	}
	
	/**
	 * Gets the ID of this relation.
	 */
	public int getID()
	{
		return this.ID;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		return this.cardinality;
	}
	
	/**
	 * Sets the result cardinality for this query.
	 * 
	 * @param cardinality The result cardinality.
	 */
	public void setOutputCardinality(long cardinality)
	{
		if (cardinality < 0) {
			throw new IllegalArgumentException();
		}
		this.cardinality = cardinality;
	}
	
	//-------------------------------------------------------------------------
	
	/**
	 * Returns an iterator over the children. Since a relation never has any children
	 * (on its level of nesting at least), this method returns an empty iterator.
	 * 
	 * @return An empty iterator;
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		List<OptimizerPlanOperator> children = Collections.emptyList();
		return children.iterator();
	}
	
	/**
	 * Gets the local predicate for this table access. The local predicate
	 * is the predicate that is evaluated over the result of this
	 * relation. In the case of a base table access, this is the
	 * actual local predicate for that table. In the case of a sub-query
	 * (nested table expression), this is the HAVING predicate.
	 * 
	 * @return The local predicate on this relation.
	 */
	public abstract LocalPredicate getPredicate();

	/**
	 * Sets the local predicate for this table access. The local predicate
	 * is the predicate that is evaluated over the result of this
	 * relation. In the case of a base table access, this is the
	 * actual local predicate for that table. In the case of a sub-query
	 * (nested table expression), this is the HAVING predicate.
	 * 
	 * @param The local predicate on this relation.
	 */
	public abstract void setPredicate(LocalPredicate predicate);

	/**
	 * Returns the column for the given name.
	 * 
	 * @param name The name of the column.
	 * @return The column for the given name or null if no such column exists.
	 */
	public abstract Column getColumn(String name);
	
}
