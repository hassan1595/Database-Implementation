package de.tuberlin.dima.minidb.optimizer;


import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;


/**
 * Optimizer representation of a FILTER operator.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FilterPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The child of this operator whose output tuples are filtered.
	 */
	private OptimizerPlanOperator childOperator;
	
	/**
	 * The simple predicate applied in this filter operator.
	 */
	private LocalPredicate localSimplePredicate;
	
	/**
	 * The cardinality of the tuple output.
	 */
	private long outCardinality;
	
	
	/**
	 * Creates a new uncorrelated FILTER operator that applies a simple predicate.
	 * 
	 * @param childOperator The child of this operator whose output tuples are filtered.
	 * @param localSimplePredicate The simple predicate applied in this filter operator.
	 */
	public FilterPlanOperator(OptimizerPlanOperator childOperator,
			LocalPredicate localSimplePredicate)
	{
		this.childOperator = childOperator;
		this.localSimplePredicate = localSimplePredicate;
		
		float selectivity = localSimplePredicate.getSelectivity();
		long outCard = (long) (childOperator.getOutputCardinality() * selectivity);
		if (outCard < 1) {
			outCard = 1;
		}
		this.outCardinality = outCard;
	}
	
	/**
	 * Creates a new uncorrelated FILTER operator that applies a simple predicate.
	 * The output cardinality for the filter is explicitly given here and not computed
	 * independently through the input cardinality and the selectivity of the
	 * applied predicate.
	 * 
	 * @param childOperator The child of this operator whose output tuples are filtered.
	 * @param localSimplePredicate The simple predicate applied in this filter operator.
	 * @param outCardinality The cardinality of the tuple output.
	 */
	public FilterPlanOperator(OptimizerPlanOperator childOperator,
			LocalPredicate localSimplePredicate,
			long outCardinality)
	{
		this.childOperator = childOperator;
		this.localSimplePredicate = localSimplePredicate;
		this.outCardinality = outCardinality;
	}
	
	
	/**
	 * Gets the child of this plan operator.
	 * 
	 * @return The child operator of the FILTER.
	 */
	public OptimizerPlanOperator getChild()
	{
		return this.childOperator;
	}

	/**
	 * Gets the simple predicate applied int his FILTER. May be null, if the filter
	 * applies no predicate.
	 * 
	 * @return The simple predicate, or null, if none is applied.
	 */
	public LocalPredicate getSimplePredicate()
	{
		return this.localSimplePredicate;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Filter";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		return Collections.singleton(this.childOperator).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		return this.outCardinality;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedTables()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		// same as for the child
		return this.childOperator.getInvolvedRelations();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		// same as for the child
		return this.childOperator.getReturnedColumns();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		// same as for the child
		return this.childOperator.getColumnOrder();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// translate the child
		PhysicalPlanOperator childPlan = this.childOperator.createPhysicalPlan(buffer, heap);
		
		// translate the predicate
		de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate pred = this.localSimplePredicate.createExecutablePredicate();
		
		return OperatorFactory.createFilterOperator(childPlan, pred);
	}

}
