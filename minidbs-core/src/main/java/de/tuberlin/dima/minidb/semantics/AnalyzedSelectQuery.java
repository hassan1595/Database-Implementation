package de.tuberlin.dima.minidb.semantics;


import java.util.HashSet;
import java.util.Set;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OrderedColumn;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;


/**
 * Simple container for the individual parts of a select query after it has been
 * semantically analyzed. A select query can itself be a relation that is
 * accessed by parent select queries.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class AnalyzedSelectQuery extends Relation
{
	/**
	 * The individual relations that the query operates on, together with the local predicates.
	 */
	private Relation[] relations;
	
	/**
	 * The edges in a graph representing the joins between the tables.
	 * Together with <tt>tableAccesses</tt>, this spans the join graph.
	 */
	private JoinGraphEdge[] joinEdges;

	/**
	 * The columns that are produced by this query. 
	 */
	private ProducedColumn[] outputColumns;
	
	/**
	 * The predicate from the HAVING clause. If this select-query is used as a
	 * nested-table expression, any local predicates on this nested-table-expression
	 * will be included here.
	 */
	private LocalPredicate havingPredicate;
	
	/**
	 * The query plan used to execute this query.
	 */
	private OptimizerPlanOperator queryPlan;
	
	/**
	 * Output column re-mapping map, used only in sub-queries.
	 */
	private int[] outColRemapping;

	/**
	 * A flag, declaring that this query groups. 
	 */
	private boolean grouping;
	
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
		
	/**
	 * Creates a new AnalyzedQuery object.
	 * 
	 * @param tableAccesses The individual tables that the query operates on,
	 *                      together with the local predicates.
	 * @param joinEdges The edges in a graph representing the joins between the tables.
	 */
	public AnalyzedSelectQuery(Relation[] relations, JoinGraphEdge[] joinEdges)
	{
		this(relations, joinEdges, null, null, false);
	}

	/**
	 * Creates a new AnalyzedQuery object.
	 * 
	 * @param tableAccesses The individual tables that the query operates on,
	 *                      together with the local predicates.
	 * @param joinEdges The edges in a graph representing the joins between the tables.
	 * @param outputColumns The columns put out by this select query.
	 * @param havingPredicate The HAVING predicate.
	 * @param grouping Flag specifying whether this query is grouping or not.
	 */
	public AnalyzedSelectQuery(Relation[] relations, JoinGraphEdge[] joinEdges, ProducedColumn[] outputColumns, LocalPredicate having, boolean grouping)
	{
		this.relations = relations;
		this.joinEdges = joinEdges;
		this.outputColumns = outputColumns;
		this.havingPredicate = having;
		this.grouping = grouping;
	}
	
	
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------

	/**
	 * Gets the individual tables that the query operates on, together with the local predicates.
	 *
	 * @return The accessed relations.
	 */
	public Relation[] getTableAccesses()
	{
		return this.relations;
	}

	/**
	 * Gets the edges in the graph representing the joins between the tables.
	 * Together with <tt>tableAccesses</tt>, this spans the join graph.
	 *
	 * @return The join edges.
	 */
	public JoinGraphEdge[] getJoinEdges()
	{
		return this.joinEdges;
	}

	/**
	 * Gets the predicate applied on top this relation.
	 * Since this relation is a select query, the predicate on top of it
	 * is the HAVING predicate.
	 * 
	 * @return The HAVING predicate.
	 * @see de.tuberlin.dima.minidb.semantics.Relation#getPredicate()
	 */
	@Override
	public LocalPredicate getPredicate()
	{
		return this.havingPredicate;
	}

	/**
	 * Sets the predicate applied on top this relation.
	 * Since this relation is a select query, the predicate on top of it
	 * is the HAVING predicate.
	 * 
	 * @param predicate The HAVING predicate.
	 * @see de.tuberlin.dima.minidb.semantics.Relation#getPredicate()
	 */
	@Override
	public void setPredicate(LocalPredicate predicate)
	{
		this.havingPredicate = predicate;	
	}
	
	/**
	 * Sets the output columns for this query.
	 * 
	 * @param columns The output columns for this query.
	 */
	public void setOutputColumns(ProducedColumn[] columns)
	{
		this.outputColumns = columns;
	}
	
	/**
	 * Gets the produced columns of this query. This method returns the columns as they are composed
	 * within this query.
	 * 
	 * @return The produced columns of this query.
	 */
	public ProducedColumn[] getOutputColumns()
	{
		return this.outputColumns;
	}

	/**
	 * Gets the output columns for this query. The columns are returned as they look for a query that
	 * accesses this query's result as a relation. That happens, when this query is a subquery. If
	 * the query has been given a re-mapping map, then the map defined in which sequence the returned columns are.
	 * 
	 * @return The output columns for this query.
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		if (this.outColRemapping == null) {
			Column[] cols = new Column[this.outputColumns.length];
			for (int i = 0; i < cols.length; i++) {
				cols[i] = new Column(this, this.outputColumns[i].getOutputDataType(), i);
			}
			return cols;
		}
		else {
			Column[] cols = new Column[this.outColRemapping.length];
			for (int i = 0; i < cols.length; i++) {
				int pos = this.outColRemapping[i];
				cols[i] = new Column(this, this.outputColumns[pos].getOutputDataType(), pos);
			}
			return cols;
		}
	}

	/**
	 * Marks this query as a grouping query. If the query is marked
	 * as a grouping query, all output columns that have no aggregation
	 * function (or <tt>NONE</tt> as their aggregation function are
	 * interpreted as grouping columns.
	 * 
	 * @param grouping Flag specifying whether this query is grouping or not.
	 */
	public void setGrouping(boolean grouping)
	{
		this.grouping = grouping;
	}
	
	/**
	 * Checks, whether this query performs grouping/aggregation.
	 *
	 * @return True, if the query is grouping and/or aggregating, false otherwise.
	 */
	public boolean isGrouping()
	{
		return this.grouping;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.semantics.Relation#getColumn(java.lang.String)
	 */
	@Override
	public Column getColumn(String name) 
	{
		// sanity check
		if(this.outputColumns == null)
		{
			return null;
		}
		// iterate over all output columns of the subquery and check if an output column with the given name exists.
		for (int i = 0; i < this.outputColumns.length; i++)
		{
			// name matches?
			if (this.outputColumns[i].getColumnAliasName().toLowerCase(Constants.CASE_LOCALE).equals(name.toLowerCase(Constants.CASE_LOCALE)))
			{
				return new ProducedColumn(this, this.outputColumns[i].getDataType(), i, this.outputColumns[i].getColumnAliasName(), this.outputColumns[i].getParsedColumn());
			}		
		}		
		// no column with such a name was found
		return null;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "SubQuery";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "SubQuery";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedRelations()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		Set<Relation> list = new HashSet<Relation>(2);
		list.add(this);
		return list;
	}

	/**
	 * Gets the query plan used to execute this query.
	 * 
	 * @return The query plan used to execute this query.
	 */
	public OptimizerPlanOperator getQueryPlan()
	{
		return this.queryPlan;
	}

	/**
	 * Sets the query plan to be used to execute this query.
	 * 
	 * @param queryPlan The query plan to be used to execute this query.
	 */
	public void setQueryPlan(OptimizerPlanOperator queryPlan)
	{
		this.queryPlan = queryPlan;
	}

	/**
	 * Creates the physical query plan that can be invoked to execute this query.
	 * If the optimizer plan for this query has not been set, this method throws
	 * an {@link IllegalStateException}.
	 * 
	 * @param buffer The buffer pool manager to be used by the physical operators that execute this query.
	 * @param heap The query heap to be used by the physical operators that execute this query.
	 * @return The physical plan used to execute this query.
	 * 
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		if (this.queryPlan == null) {
			throw new IllegalStateException();
		}
		
		return this.queryPlan.createPhysicalPlan(buffer, heap);
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{		
		return null;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.semantics.Relation#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		long card = super.getOutputCardinality();
		
		return card >= 0 ? card : (this.queryPlan == null ? -1 : this.queryPlan.getOutputCardinality());
	}
	
	/**
	 * @param outColRemapping
	 */
	public void setOutColRemapping(int[] outColRemapping)
	{
		this.outColRemapping = outColRemapping;
	}
}
