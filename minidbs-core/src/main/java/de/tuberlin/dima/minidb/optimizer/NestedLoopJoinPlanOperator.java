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
 * The optimizer representation of a Nested-Loop-Join. In this implementation, the
 * left child will become the one that feeds tuples to the outer loop and the right
 * one will feed its tuples into the inner loop.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class NestedLoopJoinPlanOperator extends AbstractJoinPlanOperator
{
	/**
	 * The columns produced by this operator.
	 */
	private Column[] outCols;
	
	/**
	 * The order of the tuples produced by this operator.
	 */
	private OrderedColumn[] producedOrder;
	
	/**
	 * The map describing how the outer input is copied to the output.
	 */
	public final int[] outerOutColMap;
	
	/**
	 * The map describing how the inner input is copied to the output.
	 */
	public final int[] innerOutColMap;
	
	

	/**
	 * Creates a new Nested-Loop-Join optimizer plan operator.
	 * The join it represents draws tuples from the outer side in the outer
	 * loop and from the inner side in the inner loop.
	 *  
	 * The tuples produced by the join operator are (in most cases) a concatenation of
	 * the tuple from outer and inner side. How the columns from the output tuple are derived
	 * from the columns of the input tuple is described in two map arrays:
	 * <tt>outerOutColMap</tt> and <tt>innerOutColMap</tt>. At position <tt>i</tt> in
	 * such a map array is the position of the column in the outer (respectively inner) tuple that
	 * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
	 * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
	 * outer tuple (respectively inner) tuple.
	 * 
	 * Here is an example of how to assign the fields of the outer tuple to the output tuple:
	 * <code>
	 * for (int i = 0; i < outerOutColMap.length; i++) {
	 *     int index = outerOutColMap[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(currentOuterTuple.getField(index), i);
	 *     }
	 * }
	 * </code>
	 * 
	 * @param outerChild The child for the outer side, whose tuples are iterated over in
	 *                   the outer loop.
	 * @param innerChild The child for the inner side, whose tuples are iterated over in
	 *                   the inner loop.
	 * @param joinPredicate The predicate to be applied during the join. It may be null, when the
	 *                      join predicate is implicitly evaluated through a correlated access in
	 *                      the inner plan.
	 * @param outerOutColMap The map that describes how the outer tuple fields are copied to the
	 *                       output tuple.  
	 * @param innerOutColMap The map that describes how the inner tuple fields are copied to the
	 *                       output tuple. 
	 * @param cardinality The expected output cardinality of the join operator.
	 */
	public NestedLoopJoinPlanOperator(OptimizerPlanOperator outerChild,
			OptimizerPlanOperator innerChild, JoinPredicate joinPredicate,
			int[] outerOutColMap, int[] innerOutColMap, long cardinality)
	{
		super(outerChild, innerChild, joinPredicate);
		
		this.outerOutColMap = outerOutColMap;
		this.innerOutColMap = innerOutColMap;
		this.cardinality = cardinality;
		
		// compose the output of this operator
		Column[] outerInputCols = this.leftChild.getReturnedColumns();
		Column[] innerInputCols = this.rightChild.getReturnedColumns();
		
		this.outCols = new Column[outerOutColMap.length];
		for (int i = 0; i < this.outCols.length; i++) {
			int leftIdx = outerOutColMap[i];
			int rightIdx = innerOutColMap[i];
			if (leftIdx == -1) {
				// must be from the right
				if (rightIdx < 0 || rightIdx >= innerInputCols.length) {
					throw new IllegalArgumentException();
				}
				this.outCols[i] = innerInputCols[rightIdx];
			}
			else if (leftIdx < 0 || leftIdx >= outerInputCols.length){
				throw new IllegalArgumentException();
			}
			else {
				this.outCols[i] = outerInputCols[leftIdx];
			}
		}
		
		// compose the produced order of the operator.
		// recall that the nested-loop-join preserves the order
		// of the outer relation
		
		OrderedColumn[] outerOrder = this.leftChild.getColumnOrder();
		
		// create the output order
		if (outerOrder != null) {
			List<OrderedColumn> outOrder = new ArrayList<OrderedColumn>();
			for (int i = 0; i < outerOrder.length; i++) {
				OrderedColumn oco = outerOrder[i].filterByColumns(this.outCols);
				if (oco != null) {
					outOrder.add(oco);
				}
			}
			this.producedOrder = outOrder.toArray(new OrderedColumn[outOrder.size()]);
		}
	}

	/**
	 * Gets the outer child of this Nested-Loop-Join operator.
	 * The outer loop iterates over the tuples of the outer child.
	 * This one corresponds to the left child.
	 * 
	 * @return The outer child sub-plan.
	 */
	public OptimizerPlanOperator getOuterChild()
	{
		return this.leftChild;
	}
	
	/**
	 * Gets the inner child of this Nested-Loop-Join operator.
	 * The inner loop iterates over the tuples of the inner child.
	 * This one corresponds to the right child.
	 * 
	 * @return The inner child sub-plan.
	 */
	public OptimizerPlanOperator getInnerChild()
	{
		return this.rightChild;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Nested-Loop Join";
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
		// translate the two children.
		PhysicalPlanOperator outerChildPlan = this.leftChild.createPhysicalPlan(buffer, heap);
		PhysicalPlanOperator innerChildPlan = this.rightChild.createPhysicalPlan(buffer, heap);
		
		// create an executable version of the join predicate
		de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate executablePredicate = this.joinPredicate == null ? null : this.joinPredicate.createExecutablepredicate();
		
		return OperatorFactory.createNestedLoopJoinOperator(outerChildPlan, innerChildPlan,
				executablePredicate, this.outerOutColMap, this.innerOutColMap);
	}
}
