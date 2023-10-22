package de.tuberlin.dima.minidb.semantics;


import java.util.HashSet;
import java.util.Set;

import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.IllegalOperationException;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.optimizer.OrderedColumn;
import de.tuberlin.dima.minidb.qexec.PhysicalPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.util.Pair;


/**
 * This class represents the access to a table that physically exists in the
 * storage. It contains the descriptor of the table, together with the local
 * predicates evaluated on it.
 * <p>
 * This table access is an optimizer plan operator, but it is an abstract 
 * operator since it does not describe how the table is accessed (via a scan
 * or through an index). It is used in the semantical analysis and during
 * stages in the optimizer where the exact physical access strategy is still
 * undefined.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class BaseTableAccess extends Relation
{
	/**
	 * Set containing a single instance of this BaseTableAccess.
	 */
	private final Set<BaseTableAccess> thisAsSet;
	
	/**
	 * The base table that is accessed.
	 */
	private final TableDescriptor table;
	
	/**
	 * The local predicate (possibly a complex one) to be evaluated on this table
	 * locally.
	 */
	private LocalPredicate predicate;
	
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new base table access descriptor for the given table.
	 * 
	 * @param table The table.
	 */
	public BaseTableAccess(TableDescriptor table)
	{
		this.table = table;
		
		this.thisAsSet = new HashSet<BaseTableAccess>(2);
		this.thisAsSet.add(this);
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the local predicate for this table access.
	 * 
	 * @return The local predicate on this table access.
	 */
	@Override
	public LocalPredicate getPredicate()
	{
		return this.predicate;
	}

	/**
	 * Sets the local predicate for this table access.
	 * 
	 * @param The local predicate on this table access to set.
	 */
	@Override
	public void setPredicate(LocalPredicate predicate)
	{
		this.predicate = predicate;
	}

	/**
	 * Gets the descriptor of the base table that is accessed.
	 * 
	 * @return The accessed base table.
	 */
	public TableDescriptor getTable()
	{
		return this.table;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.semantics.Relation#getColumn(java.lang.String)
	 */
	@Override
	public Column getColumn(String name) 
	{
		// get the schema and index for the requested column
		Pair<ColumnSchema, Integer> p = this.getTable().getSchema().getColumn(name);
		// check if column exists
		if(p == null)
		{
			return null;
		}
		// create and return column for the given name
		return new Column(this, p.getFirst().getDataType(),	p.getSecond());
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getName()
	 */
	@Override
	public String getName()
	{
		return "Base Table Access";
	}
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getInvolvedRelations()
	 */
	@Override
	public Set<Relation> getInvolvedRelations()
	{
		@SuppressWarnings("unchecked")
		Set<Relation> relSet = (Set<Relation>) (Set<?>) this.thisAsSet;
		return relSet;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return (this.table != null) ? this.table.getTableName() : "<null>";
	}

	/**
	 * Returns the columns contained in this base table access. This method
	 * returns all columns in the table, since at the points where this class
	 * is used, the exact required columns are still unknown.
	 * 
	 * @return An array with descriptors of all columns in this base table.
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#getOutputColumns()
	 */
	@Override
	public Column[] getReturnedColumns()
	{
		TableSchema tSchema = this.table.getSchema();
		int numCols = tSchema.getNumberOfColumns();
		
		Column[] cols = new Column[numCols];
		for (int i = 0; i < numCols; i++) {
			ColumnSchema schema = tSchema.getColumn(i);
			cols[i] = new Column(this, schema.getDataType(), i);
		}
		
		return cols;
	}

	/**
	 * Throws an {@link IllegalOperationException}, since this abstract operator cannot create
	 * a physical plan operator.
	 * 
	 * @see de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator#createPhysicalPlan(de.tuberlin.dima.minidb.io.BufferPoolManager, de.tuberlin.dima.minidb.qexec.heap.QueryHeap)
	 */
	@Override
	public PhysicalPlanOperator createPhysicalPlan(BufferPoolManager buffer, QueryHeap heap)
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
}
