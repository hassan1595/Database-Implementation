package de.tuberlin.dima.minidb.optimizer;


import java.util.ArrayList;
import java.util.List;

import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;


/**
 * The optimizer representation of a MERGE JOIN operator that can perform
 * inner equi-joins.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MergeJoinPlanOperator extends AbstractJoinPlanOperator
{
	/**
	 * The indices of the join columns in the left input tuple.
	 */
	public final int[] leftJoinColumns;
	
	/**
	 * The indices of the join columns in the right input tuple.
	 */
	public final int[] rightJoinColumns;
	
	/**
	 * The map describing how the left input is copied to the output.
	 */
	public final int[] leftOutColMap;
	
	/**
	 * The map describing how the right input is copied to the output.
	 */
	public final int[] rightOutColMap;
	
	/**
	 * The columns produced by this operator.
	 */
	private Column[] outCols;
	
	/**
	 * The order of the columns going out of this operator.
	 */
	private OrderedColumn[] producedOrder;
	
	

	/**
	 * Creates a new Merge Join operator that performs an inner equi-join over the
	 * given inputs, using the join columns as indicated in the arrays.
	 *  
	 * @param leftChild The left child operator.
	 * @param rightChild The right child operator.
	 * @param joinPredicate The predicate that is applied in this join.
	 * @param leftJoinCols The indices of the join columns in the left input tuple.
	 * @param rightJoinCols The indices of the join columns in the right input tuple.
	 * @param leftOutColMap The map describing how the left input is copied to the output.
	 * @param rightOutColMap The map describing how the right input is copied to the output.
	 * @param cardinality The output cardinality of the join.
	 */
	public MergeJoinPlanOperator(OptimizerPlanOperator leftChild, OptimizerPlanOperator rightChild,
			JoinPredicate joinPredicate, int[] leftJoinCols, int[] rightJoinCols,
			int[] leftOutColMap, int[] rightOutColMap, long cardinality)
	throws OptimizerException
	{ 
		super(leftChild, rightChild, joinPredicate);
		
		this.leftJoinColumns = leftJoinCols;
		this.rightJoinColumns = rightJoinCols;
		this.leftOutColMap = leftOutColMap;
		this.rightOutColMap = rightOutColMap;
		this.cardinality = cardinality;
		
		// produce the output columns
		if (leftOutColMap.length != rightOutColMap.length) {
			throw new OptimizerException("Output columns maps are invalid.");
		}
		
		Column[] leftInputCols = leftChild.getReturnedColumns();
		Column[] rightInputCols = rightChild.getReturnedColumns();
		
		this.outCols = new Column[leftOutColMap.length];
		for (int i = 0; i < this.outCols.length; i++) {
			int leftIdx = leftOutColMap[i];
			int rightIdx = rightOutColMap[i];
			if (leftIdx == -1) {
				// must be from the right
				if (rightIdx < 0 || rightIdx >= rightInputCols.length) {
					throw new IllegalArgumentException();
				}
				this.outCols[i] = rightInputCols[rightIdx];
			}
			else if (leftIdx < 0 || leftIdx >= leftInputCols.length){
				throw new IllegalArgumentException();
			}
			else {
				this.outCols[i] = leftInputCols[leftIdx];
			}
		}
		
		if (leftJoinCols.length != rightJoinCols.length) {
			throw new IllegalArgumentException("Join column index arrays do not match.");
		}
		
		// check that the input if ordered on the key columns
		OrderedColumn[] leftOrder = leftChild.getColumnOrder();
		OrderedColumn[] rightOrder = rightChild.getColumnOrder();
		
		if (leftOrder.length < leftJoinCols.length) {
			throw new OptimizerException(
					"Required order for merge join not provided on left hand side.");
		}
		else if (rightOrder.length < rightJoinCols.length) {
			throw new OptimizerException(
					"Required order for merge join not provided on right hand side.");
		}
		
		// create the output order
		List<OrderedColumn> outOrder = new ArrayList<OrderedColumn>();
		for (int i = 0; i < leftJoinCols.length; i++) {
			Column leftCol = leftInputCols[leftJoinCols[i]];
			Column rightCol = rightInputCols[rightJoinCols[i]];
			// check that the column is part of the order
			if (!(leftOrder[i].containsColumn(leftCol) && rightOrder[i].containsColumn(rightCol))) {
				throw new OptimizerException("Join column " + i + " is not ordered for merge join.");
			}
			
			OrderedColumn leftOutOrder = leftOrder[i].filterByColumns(this.outCols);
			OrderedColumn rightOutOrder = rightOrder[i].filterByColumns(this.outCols);
			if (leftOutOrder != null) {
				if (rightOutOrder != null) {
					outOrder.add(new OrderedColumn(leftOutOrder, rightOutOrder));
				}
				else {
					outOrder.add(leftOutOrder);
				}
			}
			else if (rightOutOrder != null) {
				outOrder.add(rightOutOrder);
			}
		}
		this.producedOrder = outOrder.toArray(new OrderedColumn[outOrder.size()]);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Merge Join";
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		return this.outCols;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{

		return this.producedOrder;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// translate the children to a physical plan
		PhysicalPlanOperator leftPlan = this.leftChild.createPhysicalPlan(buffer, heap);
		PhysicalPlanOperator rightPlan = this.rightChild.createPhysicalPlan(buffer, heap);
		
		// create the merge join operator
		return OperatorFactory.createMergeJoinOperator(leftPlan, rightPlan, this.leftJoinColumns,
				this.rightJoinColumns, this.leftOutColMap, this.rightOutColMap);
	}
}
