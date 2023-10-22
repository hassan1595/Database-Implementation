package de.tuberlin.dima.minidb.optimizer;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import de.tuberlin.dima.minidb.core.IllegalOperationException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * The optimizer representation of an abstract join between two sets of tables.
 * This join is abstract in the sense that it does not yet have a specific
 * algorithm assigned, or inputs, outputs, orders, etc. It can hence make no
 * statement about any costs.
 * 
 * Its primary use is the creation of join trees that are derived based on
 * cardinalities and not any other form of costs.  
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class AbstractJoinPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The left child.
	 */
	protected OptimizerPlanOperator leftChild;
	
	/**
	 * The right child.
	 */
	protected OptimizerPlanOperator rightChild;
	
	/**
	 * The join predicate applied during this join.
	 */
	protected JoinPredicate joinPredicate;
	
	/**
	 * An array of tables that are involved in the plans below this join.
	 */
	protected HashSet<Relation> involvedRelations;
	
	/**
	 * The cardinality of the operator.
	 */
	protected long cardinality;
	
	
	/**
	 * Creates a new abstract join representation with the given children
	 * and the given predicate. The assignment of the children must match
	 * the join predicate direction.
	 * 
	 * @param leftChild The left child. 
	 * @param rightChild The right child.
	 * @param joinPredicate The join predicate applied during this join.
	 */
	public AbstractJoinPlanOperator(OptimizerPlanOperator leftChild,
			                        OptimizerPlanOperator rightChild,
			                        JoinPredicate joinPredicate)
	{
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.joinPredicate = joinPredicate;
	}
	

	/**
	 * Gets the leftChild from this AbstractJoinPlanOperator.
	 *
	 * @return The leftChild.
	 */
	public OptimizerPlanOperator getLeftChild()
	{
		return this.leftChild;
	}

	/**
	 * Gets the rightChild from this AbstractJoinPlanOperator.
	 *
	 * @return The rightChild.
	 */
	public OptimizerPlanOperator getRightChild()
	{
		return this.rightChild;
	}
	
	/**
	 * Sets the output cardinality for the join.
	 * 
	 * @param cardinality The cardinality to set.
	 */
	public void setOutputCardinality(long cardinality)
	{
		this.cardinality = cardinality;
	}

	/**
	 * Gets the joinPredicate from this AbstractJoinPlanOperator.
	 *
	 * @return The joinPredicate.
	 */
	public JoinPredicate getJoinPredicate()
	{
		return this.joinPredicate;
	}

	/**
	 * Sets the joinPredicate from this AbstractJoinPlanOperator.
	 *
	 * @return The joinPredicate.
	 */
	public void setJoinPredicate(JoinPredicate p)
	{
		this.joinPredicate = p;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Abstract Join Plan Operator";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		return this.cardinality;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		List<OptimizerPlanOperator> list = new ArrayList<OptimizerPlanOperator>(2);
		list.add(this.leftChild);
		list.add(this.rightChild);
		return list.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedTables()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		if (this.involvedRelations == null) {
			Set<Relation> leftTabs = this.leftChild.getInvolvedRelations();
			Set<Relation> rightTabs = this.rightChild.getInvolvedRelations();
			HashSet<Relation> thisTabs = new HashSet<Relation>(leftTabs.size() + rightTabs.size());
			
			// union the sets
			thisTabs.addAll(rightTabs);
			thisTabs.addAll(leftTabs);
			
			this.involvedRelations = thisTabs;
			return thisTabs;
		}
		else {
			return this.involvedRelations;
		}
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder();
		
		bld.append(this.joinPredicate).append("  -  LEFT: [");
		for (Relation r : this.leftChild.getInvolvedRelations()) {
			bld.append('(').append(r.toString()).append(')');
		}
		
		bld.append("]  -  RIGHT [");
		for (Relation r : this.rightChild.getInvolvedRelations()) {
			bld.append('(').append(r.toString()).append(')');
		}
		bld.append(']');
		
		return bld.toString();
	}


	/**
	 * Gets the output columns of this abstract join. Since the join operator is
	 * abstract and purely for an intermediate representation of the join order,
	 * it never returns output columns, but always throws an
	 * {@link IllegalOperationException}.
	 * 
	 * @return This method never returns normally.
	 * @throws IllegalOperationException Thrown whenever this method is invoked.
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		throw new IllegalOperationException();
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		return null;
	}


	/**
	 * Since the join operator is abstract and purely for an intermediate representation 
	 * of the join order, it never returns output columns, but always throws an
	 * {@link IllegalOperationException}.
	 * 
	 * @param buffer Unused.
	 * @param heap Unused.
	 * @return This method never returns normally.
	 * @throws IllegalOperationException Thrown whenever this method is invoked.
	 * 
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		throw new IllegalOperationException();
	}
}
