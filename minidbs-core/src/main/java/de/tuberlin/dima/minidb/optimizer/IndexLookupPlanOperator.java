package de.tuberlin.dima.minidb.optimizer;


import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.InternalOperationFailure;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.parser.Predicate;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Order;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateAtom;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicateBetween;


/**
 * The optimizer plan representation of an index lookup, correlated or
 * uncorrelated.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexLookupPlanOperator extends OptimizerPlanOperator
{
	/**
	 * The descriptor for the index that this scan operates on.
	 */
	private final IndexDescriptor theIndex;
	
	/**
	 * The table access for which stand.
	 */
	private final BaseTableAccess tableAccess;
	
	/**
	 * The column accessed by the index.
	 */
	private final Column indexedColumn;
	
	/**
	 * The local predicate passed to the constructor.
	 */
	private final LocalPredicate pred;
	
	/**
	 * The lower bound key or equality key, depending on the mode. 
	 */
	private final DataField key1;
	
	/**
	 * The upper bound key, or null, if only equality is evaluated. 
	 */
	private final DataField key2;
	
	/**
	 * Flag indicating that the lower bound is included in the range. 
	 */
	private final boolean key1Included;
	
	/**
	 * Flag indicating that the upper bound is included in the range.
	 */
	private final boolean key2Included;
	
	/**
	 * The output cardinality of the index scan.
	 */
	private final long outCardinality;
	
	/**
	 * The index for the correlated access. Is -1, if the index scan is not correlated.
	 */
	private final int correlatedColumnIndex;
	
	
	
	/**
	 * Creates a new index scan operator that operates uncorrelated 
	 * and evaluates the given predicate.
	 * 
	 * @param index The index descriptor for the index that is to scan.
	 * @param table The table access for which the index scan stands.
	 * @param pred The predicate to be applied.
	 */
	public IndexLookupPlanOperator(IndexDescriptor index, BaseTableAccess table,
			LocalPredicateAtom pred, long outCard)
	{
		this(index, table, pred, outCard, false);
	}
	
	/**
	 * Creates a new index scan operator that operates uncorrelated 
	 * and evaluates the given predicate.
	 * 
	 * @param index The index descriptor for the index that is to scan.
	 * @param table The table access for which the index scan stands.
	 * @param pred The predicate to be applied.
	 */
	public IndexLookupPlanOperator(IndexDescriptor index, BaseTableAccess table,
			LocalPredicateBetween pred, long outCard)
	{
		this(index, table, pred, outCard, false);
	}
	
	
	
	private IndexLookupPlanOperator(IndexDescriptor index, BaseTableAccess table,
			LocalPredicate pred, long outCard, boolean marker)
	{
		this.theIndex = index;
		this.tableAccess = table;
		this.outCardinality = outCard;
		this.pred = pred;
		
		// no correlation
		this.correlatedColumnIndex = -1;
		
		DataType indexedColumnType = index.getSchema().getIndexedColumnSchema().getDataType();
		
		this.indexedColumn = new Column(table, indexedColumnType, index.getSchema().getColumnNumber());
		
		if (pred instanceof LocalPredicateAtom) {
			// atom, we may have equality or range
			LocalPredicateAtom atom = (LocalPredicateAtom) pred;
			switch (atom.getParsedPredicate().getOp())
			{
			case EQUAL:
				this.key1 = atom.getLiteral();
				this.key2 = null;
				this.key1Included = true;
				this.key2Included = false;
				break;
			case GREATER:
				this.key1 = atom.getLiteral();
				this.key1Included = false;
				this.key2 = indexedColumnType.getMaxValue();
				this.key2Included = true;
				break;
			case GREATER_OR_EQUAL:
				this.key1 = atom.getLiteral();
				this.key1Included = true;
				this.key2 = indexedColumnType.getMaxValue();
				this.key2Included = true;
				break;
			case SMALLER:
				this.key1 = indexedColumnType.getMinValue();
				this.key1Included = true;
				this.key2 = atom.getLiteral();
				this.key2Included = false;
				break;
			case SMALLER_OR_EQUAL:
				this.key1 = indexedColumnType.getMinValue();
				this.key1Included = true;
				this.key2 = atom.getLiteral();
				this.key2Included = true;
				break;
			default:
				throw new IllegalArgumentException("Index cannot answer inequality predicate queries.");
			}
		}
		else if (pred instanceof LocalPredicateBetween) {
			// between predicate, so we have a range
			LocalPredicateBetween bet = (LocalPredicateBetween) pred;
			
			this.key1 = bet.getLowerBoundLiteral();
			this.key2 = bet.getUpperBoundLiteral();
			
			if (bet.getLowerBound().getOp() == Predicate.Operator.GREATER) {
				this.key1Included = false;
			}
			else if (bet.getLowerBound().getOp() == Predicate.Operator.GREATER_OR_EQUAL) {
				this.key1Included = true;
			}
			else {
				throw new InternalOperationFailure(
						"Invalid operator in between predicate lower bound.", false, null);
			}
			
			if (bet.getUpperBound().getOp() == Predicate.Operator.SMALLER) {
				this.key2Included = false;
			}
			else if (bet.getUpperBound().getOp() == Predicate.Operator.SMALLER_OR_EQUAL) {
				this.key2Included = true;
			}
			else {
				throw new InternalOperationFailure(
						"Invalid operator in between predicate upper bound.", false, null);
			}
		}
		else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Creates an index lookup operator for correlated index accessed.
	 * 
	 * @param index The index that is accessed.
	 * @param table The base table that is accessed.
	 * @param correlatedColumnIndex The column number of the correlated column.
	 * @param cardinalityPerAccess The average number of rows returned per correlated access.
	 */
	public IndexLookupPlanOperator(IndexDescriptor index, BaseTableAccess table,
			int correlatedColumnIndex, long cardinalityPerAccess)
	{
		this.theIndex = index;
		this.tableAccess = table;
		this.correlatedColumnIndex = correlatedColumnIndex;
		this.outCardinality = cardinalityPerAccess;
		this.pred = null;
		
		DataType indexedColumnType = index.getSchema().getIndexedColumnSchema().getDataType();
		this.indexedColumn = new Column(table, indexedColumnType, index.getSchema().getColumnNumber());
		
		this.key1 = null;
		this.key2 = null;
		this.key1Included = false;
		this.key2Included = false;
	}
	
	/**
	 * Gets the index descriptor from this IndexScanPlanOperator.
	 *
	 * @return The index descriptor.
	 */
	public IndexDescriptor getIndex()
	{
		return this.theIndex;
	}

	/**
	 * Gets the table for this IndexScanPlanOperator.
	 *
	 * @return The table.
	 */
	public BaseTableAccess getTableAccess()
	{
		return this.tableAccess;
	}
	
	/**
	 * Get the local predicate passed to the constructor
	 * 
	 * @return LocalPredicate 
	 */
	public LocalPredicate getLocalPredicate()
	{
		return this.pred;
	}
	
	/**
	 * Checks, if this index access is correlated, such as in an index-nested-loop-join.
	 * 
	 * @return True, if this Index-Scan performs a correlated access.
	 */
	public boolean isCorrelated()
	{
		return this.correlatedColumnIndex >= 0;
	}
	
	/**
	 * Returns the index of the correlated column or -1 if the operator is not correlated.
	 * 
	 * @return the index of the correlated column.
	 */
	public int getCorrelatedColumnIndex()
	{
		return isCorrelated() ? this.correlatedColumnIndex : -1;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Index Scan";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getChildren()
	 */
	@Override
	public Iterator<OptimizerPlanOperator> getChildren()
	{
		return Collections.<OptimizerPlanOperator>emptyList().iterator();
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
		Set<Relation> list = new HashSet<Relation>(2);
		list.add(this.tableAccess);
		return list;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		return new Column[] { new Column(this.tableAccess, DataType.ridType(), Column.RID_COLUMN_INDEX) };
	}

//	/* (non-Javadoc)
//	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
//	 */
//	@Override
//	public OutputColumnOrder[] getColumnOrder()
//	{
//		// the order is after a column that we do not yet have fetched
//		OptimizerColumn col = new OptimizerColumn(tableAccess, 
//				theIndex.getSchema().getColumnNumber());
//		return new OutputColumnOrder[] { new OutputColumnOrder(col, true) };
//	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// get the index that the operator works on
		BTreeIndex index = AbstractExtensionFactory.getExtensionFactory().createBTreeIndex(this.theIndex.getSchema(),
				buffer, this.theIndex.getResourceId());
		
		if (isCorrelated()) {
			return OperatorFactory.createIndexCorrelatedLookupOperator(index, this.correlatedColumnIndex);
		}
		else {
			// not operating correlated. check which kind of predicate
			if (this.key2 == null) {
				return OperatorFactory.createIndexScanOperatorForEqualityPredicate(index, this.key1);
			}
			else {
				return OperatorFactory.createIndexScanOperatorForBetweenPredicate(
						index, this.key1, this.key1Included, this.key2, this.key2Included);
			}
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getColumnOrder()
	 */
	@Override
	public OrderedColumn[] getColumnOrder()
	{
		return new OrderedColumn[] { new OrderedColumn(this.indexedColumn, Order.ASCENDING) };
	}

}
