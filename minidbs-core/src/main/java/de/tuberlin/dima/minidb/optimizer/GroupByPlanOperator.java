package de.tuberlin.dima.minidb.optimizer;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;
import de.tuberlin.dima.minidb.semantics.Relation;


/**
 * The optimizer plan operator representing a group by operation. This operator contains all
 * information required to create a physical group by operator.
 * <p>
 * The group by operator is expected to receive tuples where the order of columns in the
 * tuples is that of the output tuples.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class GroupByPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The child below this plan operator.
	 */
	private OptimizerPlanOperator child;
	
	/**
	 * The columns that are produced by this operator.
	 */
	private ProducedColumn[] prodCols;
	
	/**
	 * The indices of the grouping columns in the input tuple.
	 */
	private int[] groupColIndices;
	
	/**
	 * The indices of the aggregation columns in the input tuple.
	 */
	private int[] aggColIndices;
	
	/**
	 * The columns produced by this operator.
	 */
	private Column[] outColumns;
	
	/**
	 * The order of the tuples as they leave the operator.
	 */
	private OrderedColumn[] order;
	
	/**
	 * The cardinality of this group by operator.
	 */
	private long cardinality;
	
	/**
	 * The group by operator is expected to receive tuples where the order of columns in the
	 * tuples is that of the output tuples.
	 * 
	 * @param outCols
	 * @param groupColIndices
	 * @param aggColIndices
	 * @param outCardinality
	 */
	public GroupByPlanOperator(OptimizerPlanOperator child, ProducedColumn[] outCols,
			int[] groupColIndices, int[] aggColIndices, int outCardinality)
	throws OptimizerException
	{
		this.child = child;
		this.prodCols = outCols;
		this.groupColIndices = groupColIndices;
		this.aggColIndices = aggColIndices;
		this.cardinality = outCardinality;
		
		// set the output columns
		this.outColumns = new Column[outCols.length];
		for (int i = 0; i < outCols.length; i++) {
			this.outColumns[i] = new Column(outCols[i].getRelation(), outCols[i].getOutputDataType(), i);
		}
		
		Column[] inputCols = child.getReturnedColumns();
		OrderedColumn[] childOrder = child.getColumnOrder();
		
		// the group by operator preserves the order of the
		// grouping columns only
		int childLength = childOrder == null ? 0 : childOrder.length;
		if (childLength < groupColIndices.length) {
			throw new OptimizerException(
					"Group By Operator has input not sorted on all grouping columns");
		}

		// check that every grouping column is actually in the child order
		// and preserve those that are in the output
		boolean[] checkedInCols = new boolean[groupColIndices.length];
		
		// collect the output column order specification
		List<OrderedColumn> orderCols = new ArrayList<OrderedColumn>();
		for (int i = 0; i < childLength; i++)
		{
			OrderedColumn thisOrderCol = childOrder[i];
			int tuplePos = -1;
		
			// find the ordered column in the input columns
			for (int k = 0; k < this.outColumns.length; k++) {
				if (thisOrderCol.containsColumn(inputCols[k])) {
					tuplePos = k;
					break;
				}
			}
			
			if (tuplePos == -1) {
				throw new OptimizerException("Group By found the description of an order on a column not produced by its child.");
			}
			
			// check the group col index
			for (int k = 0; k < groupColIndices.length; k++) {
				if (groupColIndices[k] == tuplePos) {
					checkedInCols[k] = true;
					break;
				}
			}
			
			// find the index of the output
			orderCols.add(thisOrderCol);
		}
		
		// check that there really is an order over all grouping columns
		for (int i = 0; i < checkedInCols.length; i++) {
			if (!checkedInCols[i]) {
				throw new OptimizerException("Group by Operator encountered a grouping column over which no order exists.");
			}
		}
		
		this.order = orderCols.toArray(new OrderedColumn[orderCols.size()]);
	}
	
	/**
	 * Gets the child from this GroupByPlanOperator.
	 *
	 * @return The child.
	 */
	public OptimizerPlanOperator getChild()
	{
		return this.child;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Group By";
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
		return Collections.singleton(this.child).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedTables()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		return this.child.getInvolvedRelations();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		return this.outColumns;
	}
	
	public ProducedColumn[] getProducedColumns()
	{
		return this.prodCols;
	}
	
	public int[] getGroupColIndices()
	{
		return this.groupColIndices;
	}
	
	public int[] getAggColIndices()
	{
		return this.aggColIndices;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		return this.order;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// translate the child first
		PhysicalPlanOperator childPlan = this.child.createPhysicalPlan(buffer, heap);
		
		// build the arrays with the aggregate functions and the aggregate data types
		OutputColumn.AggregationType[] aggFunct = new OutputColumn.AggregationType[this.aggColIndices.length];
		DataType[] aggType = new DataType[this.aggColIndices.length];
		for (int i = 0; i < this.aggColIndices.length; i++) {
			ProducedColumn pc = this.prodCols[this.aggColIndices[i]];
			aggFunct[i] = pc.getAggregationFunction();
			aggType[i] = pc.getOutputDataType();
		}
		
		// build the arrays with the aggregate and grouping output positions
		int[] groupOutPos = new int[this.prodCols.length];
		int[] aggOutPos = new int[this.prodCols.length];
		for (int i = 0, gi = 0, ai = 0; i < groupOutPos.length; i++) {
			ProducedColumn pc = this.prodCols[i];
			if (pc.getAggregationFunction() == OutputColumn.AggregationType.NONE) {
				groupOutPos[i] = gi++;
				aggOutPos[i] = -1;
			}
			else {
				groupOutPos[i] = -1;
				aggOutPos[i] = ai++;
			}
		}
		
		// now add this operator
		return OperatorFactory.createGroupByOperator(childPlan, this.groupColIndices, this.aggColIndices,
				aggFunct, aggType, groupOutPos, aggOutPos);
	}

}
