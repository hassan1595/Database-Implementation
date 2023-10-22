package de.tuberlin.dima.minidb.optimizer;


import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Order;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * The optimizer plan operator for a sort operation.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SortPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The child of this operator.
	 */
	private OptimizerPlanOperator childOperator;
	
	/**
	 * The order of the columns produced by this sort.
	 */
	private OrderedColumn[] producedOrder;
	
	/**
	 * The indices in the input tuple of the columns to sort.
	 */
	private int[] sortColumnIndices;
	
	/**
	 * The flags indicating the sort direction.
	 * True indicates ascending order, false indicates descending order.
	 */
	private boolean[] sortAscending;

	
	/**
	 * Creates a new sort operator that sorts the tuples produced by the given child.
	 * The sort can be across multiple columns, in different directions, as indicated
	 * by the given arrays.
	 * 
	 * @param child The child whose produced tuples are to be sorted.
	 * @param sortColumnIndices The indices of the columns after which to sort.
	 * @param sortAscending The direction of sorting for each column. True indicates ascending,
	 *                      false indicates descending.
	 */
	public SortPlanOperator(OptimizerPlanOperator child,
			int[] sortColumnIndices, boolean[] sortAscending)
	{
		this.childOperator = child;
		this.sortColumnIndices = sortColumnIndices;
		this.sortAscending = sortAscending;
		
		if (sortColumnIndices.length != sortAscending.length) {
			throw new IllegalArgumentException("Sort parameter arrays do not match in length.");
		}
		
		Column[] outcols = child.getReturnedColumns();
		this.producedOrder = new OrderedColumn[sortColumnIndices.length];
		
		for (int i = 0; i < sortColumnIndices.length; i++) {
			int index = sortColumnIndices[i];
			if (index < 0 || index >= outcols.length) {
				throw new IllegalArgumentException("The given order specifies a tuple index " +
						"beyond the width of the input tuples.");
			}
			this.producedOrder[i] = new OrderedColumn(outcols[index], sortAscending[i] ? Order.ASCENDING : Order.DESCENDING);
		}
	}
	
	/**
	 * Gets the child of this Sort Operator.
	 * 
	 * @return This operator's child.
	 */
	public OptimizerPlanOperator getChild()
	{
		return this.childOperator;
	}
	
	/**
	 * Returns the sort column indices for this sort operator.
	 * 
	 * @return
	 */
	public int[] getSortColumnIndices()
	{
		return this.sortColumnIndices;
	}
	
	/**
	 * Returns the sort ascending flags for this sort operator.
	 * 
	 * @return
	 */
	public boolean[] getSortAscending()
	{
		return this.sortAscending;
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		// only one child
		return Collections.singleton(this.childOperator).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		return this.producedOrder;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Sort";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputCardinality()
	 */
	@Override
	public long getOutputCardinality()
	{
		// this operator does not change cardinality, since we do not do any
		// de-duplication or so
		return this.childOperator.getOutputCardinality();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedTables()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		return this.childOperator.getInvolvedRelations();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		// the output columns are the same as for the child
		return this.childOperator.getReturnedColumns();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// recursively create the child plan
		PhysicalPlanOperator childPlan = this.childOperator.createPhysicalPlan(buffer, heap);
		
		// assemble the schema information for the input tuples
		Column[] inputCols = this.childOperator.getReturnedColumns();
		
		DataType[] tupleSchema = new DataType[inputCols.length];
		for (int i = 0; i < inputCols.length; i++) {
			tupleSchema[i] = inputCols[i].getDataType();
		}
		
		long card = this.childOperator.getOutputCardinality();
		int intCard = card <= Integer.MAX_VALUE ? (int) card : Integer.MAX_VALUE;
		
		return OperatorFactory.createSortOperator(childPlan, heap, tupleSchema,
				intCard, this.sortColumnIndices, this.sortAscending);
	}

}
