package de.tuberlin.dima.minidb.optimizer;


import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.semantics.BaseTableAccess;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.qexec.OperatorFactory;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicateConjunction;


/**
 * Plan operator describing a table access via a full table scan.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableScanPlanOperator extends BaseTableAccess
{
	/**
	 * The columns produced by this operator.
	 */
	private Column[] producedColumns;
	
	/**
	 * The base table access node for the fetched table.
	 */
	private BaseTableAccess tableAccess;
	
	/**
	 * The input cardinality of the table scan.
	 */
	private long inputCardinality;
	
	/**
	 * The number of pages to prefetch during scan operations.
	 */
	private int prefetchingLength = Constants.DEFAULT_PREFETCHING_LENGTH;
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new plain table scan plan operator without any column
	 * specification. This constructor is for setting up a table for the
	 * The operator has initially abstract plan to determine join orders.
	 * The table scan has initially no predicate and no costs associated with it. 
	 * 
	 * @param baseTable The table to be scanned.
	 */
	public TableScanPlanOperator(BaseTableAccess access, Column[] producedCols)
	{
		super(access.getTable());
		this.tableAccess = access;
		
		setPredicate(access.getPredicate());
		setOutputCardinality(access.getOutputCardinality());
		
		this.producedColumns = producedCols;
		
		long inRows = access.getTable().getStatistics().getCardinality();
		setInputCardinaliy(inRows > 0 ? inRows : Constants.DEFAULT_TABLE_CARDINALITY);
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Gets the table from which this operator fetches the tuples.
	 * 
	 * @return The table from which this operator fetches the tuples.
	 */
	public BaseTableAccess getTableAccess()
	{
		return this.tableAccess;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Table Scan";
	}
	
	/**
	 * Gets the input cardinality of this operator.
	 * 
	 * @return The input cardinality of this operator.
	 */
	public long getInputCardinality()
	{
		return this.inputCardinality;
	}
	
	/**
	 * Sets the input cardinality for this table scan.
	 * 
	 * @param cardinality The new input cardinality.
	 */
	public void setInputCardinaliy(long cardinality)
	{
		this.inputCardinality = cardinality;
	}

	/**
	 * Gets the prefetchingLength from this TableScanPlanOperator.
	 *
	 * @return The prefetchingLength.
	 */
	public int getPrefetchingLength()
	{
		return this.prefetchingLength;
	}

	/**
	 * Sets the prefetchingLength for this TableScanPlanOperator.
	 *
	 * @param prefetchingLength The prefetchingLength to set.
	 */
	public void setPrefetchingLength(int prefetchingLength)
	{
		this.prefetchingLength = prefetchingLength;
	}
	
	/**
	 * Assigns this operator the columns it should produce.
	 * 
	 * @param outCols The columns that the table scan should produce.
	 */
	public void assignOutputColumns(Column[] outCols)
	{
		this.producedColumns = outCols;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		return this.producedColumns;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
	{
		// check if we have columns assigned. if not, the scan is still abstract and cannot
		// be translated into a physical plan
		if (this.producedColumns == null) {
			throw new IllegalStateException(
					"Table Scan Plan Operator is still abstract (has no columns assigned).");
		}
		
		// first translate the predicate
		LocalPredicate pred = getPredicate();
		LowLevelPredicate[] execPred = null;
		
		// because the table scan directly expects an array of low level predicate
		// for efficiency, we need to do some case distinctions
		if (pred == null) {
			execPred = new LowLevelPredicate[0];
		}
		else {
			de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate qPred =
				pred.createExecutablePredicate();
			
			if (qPred instanceof LowLevelPredicate) {
				execPred = new LowLevelPredicate[] { (LowLevelPredicate) qPred };
			}
			else if (qPred instanceof LocalPredicateConjunction) {
				de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate[] factors =
					((LocalPredicateConjunction) qPred).getFactors();
				execPred = new LowLevelPredicate[factors.length];
				for (int factor = 0; factor < factors.length; factor++) {
					if (!(factors[factor] instanceof LowLevelPredicate)) {
						throw new IllegalArgumentException(
								"Physical plan generation encountered a predicate it cannot handle: " + pred);
					}
					execPred[factor] = (LowLevelPredicate) factors[factor];
				}
			}
			else {
				throw new IllegalArgumentException(
						"Physical plan generation encountered a predicate it cannot handle: " + pred);
			}
		}
		
		// build the array of columns to produce
		int[] colIndices = new int[this.producedColumns.length];
		for (int i = 0; i < colIndices.length; i++) {
			colIndices[i] = this.producedColumns[i].getColumnIndex();
		}
		
		return OperatorFactory.createTableScanOperator(buffer, getTable().getResourceManager(),
				getTable().getResourceId(), colIndices, execPred, this.prefetchingLength);
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(128);
		bld.append('[').append(getName()).append(' ');
		bld.append(':').append(' ');
		bld.append(getTable().getTableName());
		bld.append(" IN: ").append(getInputCardinality());
		bld.append(", OUT: ").append(getOutputCardinality()).append(']');
		return bld.toString();
	}
	
}
