package de.tuberlin.dima.minidb.catalogue;


import java.io.IOException;

import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndexPage;
import de.tuberlin.dima.minidb.io.index.IndexResourceManager;


/**
 * A simple description of the schema of an index. In contrast to a table schema, it
 * is not completely static, but allows the the root node and first leaf node to change.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexSchema
{
	/**
	 * The schema of the table that is indexed by the index described here. 
	 */
	private TableSchema indexedTable;
	
	/**
	 * The number of bytes per page in this index.
	 */
	private PageSize pageSize;
	
	/**
	 * The column indexed by this index.
	 */
	private int indexedColumn;

	/**
	 * Gets the fan-out (degree) of the B-Tree, i.e. the number of keys in internal nodes.
	 */
	private int fanOut;
	
	/**
	 * The maximal number of key / RID pairs in the leaf pages.
	 */
	private int maximalLeafEntries;
	
	/**
	 * Flag indicating that the schema describes a unique index.
	 */
	private boolean unique;
	
	/**
	 * The page number of the root page.
	 */
	private int rootPageNumber;
	
	/**
	 * The page number of the first page.
	 */
	private int firstLeafNumber;
	
	/**
	 * Resource manager to be notified when the index schema changes.
	 */
	private IndexResourceManager toNotifyOnChanges;
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new index schema for an index over the table described by the
	 * given schema. The index is not unique and has the default page size.
	 * 
	 * Root node and first leaf node are set to 1 (behind the minimal header).
	 * 
	 * @param indexedTable The schema of the table that is indexed.
	 * @param column The number (starting at 0) of the column that is indexed.
	 */
	public IndexSchema(TableSchema indexedTable, int column)
	{
		this(indexedTable, column, PageSize.getDefaultPageSize(), false, 1, 1);
	}
	
	/**
	 * Creates a new index schema for an index over the table described by the
	 * given schema indexing the given columns. The index has the default page size.
	 * 
	 * Root node and first leaf node are set to 1 (behind the minimal header).
	 * 
	 * @param indexedTable The schema of the table that is indexed.
	 * @param column The number (starting at 0) of the column that is indexed.
	 * @param unique Whether the entries in this index should be unique.
	 */
	public IndexSchema(TableSchema indexedTable, int column, boolean unique)
	{
		this(indexedTable, column, PageSize.getDefaultPageSize(), unique, 1, 1);
	}
	
	/**
	 * Creates a new index schema for an index over the table described by the
	 * given schema indexing the given columns. The nodes of the index are pages
	 * of the given size. The index is not unique.
	 * 
	 * Root node and first leaf node are set to 1 (behind the minimal header).
	 * 
	 * @param indexedTable The schema of the table that is indexed.
	 * @param column The number (starting at 0) of the column that is indexed.
	 * @param pageSize The size of the pages that hold the index data.
	 */
	public IndexSchema(TableSchema indexedTable, int column, PageSize pageSize)
	{
		this(indexedTable, column, pageSize, false, 1, 1);
	}
	
	/**
	 * Creates a new index schema for an index over the table described by the
	 * given schema indexing the given columns. The nodes of the index are pages
	 * of the given size.
	 * 
	 * Root node and first leaf node are set as given.
	 * 
	 * @param indexedTable The schema of the table that is indexed.
	 * @param column The number (starting at 0) of the column that is indexed.
	 * @param pageSize The size of the pages that hold the index data.
	 * @param unique Whether the entries in this index should be unique.
	 * @param rootNode The page number of the root node.
	 * @param firstLeafNode The page number of the first (left most) leaf node.
	 */
	public IndexSchema(TableSchema indexedTable, int column, PageSize pageSize, boolean unique,
			           int rootNode, int firstLeafNode)
	{
		if (indexedTable == null) {
			throw new NullPointerException("The indexed table must not be null");
		}
		if (column < 0 || column >= indexedTable.getNumberOfColumns()) {
			throw new IllegalArgumentException("Column out of range for the table.");
		}
		if (pageSize == null) {
			throw new NullPointerException("Page size must not be null");
		}
		if (rootNode < 1 || firstLeafNode < 1) {
			throw new IllegalArgumentException("Page numbers for data pages must be greater than zero");
		}
		
		// compute the fan-out
		// the length of one entry is key + pageNumber
		int len = 0; // for the page number as the reference
		try {
			ColumnSchema cs = indexedTable.getColumn(column);
			if (!cs.getDataType().isFixLength()) {
				throw new IllegalArgumentException("Column " + column + " is not a fix length data type.");
			}
			len = cs.getDataType().getNumberOfBytes();
		}
		catch (IndexOutOfBoundsException ioobex) {
			throw new IllegalArgumentException("Indexed table does not contain column "
					+ column);
		}
		
		// the tree fan-out is the number of keys per page
		// subtract from the page the header and the one extra pointer
		this.fanOut = (pageSize.getNumberOfBytes() - BTreeIndexPage.INDEX_PAGE_HEADER_SIZE - 4) / (len + 4);
		this.maximalLeafEntries = (pageSize.getNumberOfBytes() - BTreeIndexPage.INDEX_PAGE_HEADER_SIZE) /
		                          (len + RID.getRIDSize()); 
		
		if (this.fanOut < 4 || this.maximalLeafEntries < 4) {
			throw new IllegalArgumentException("The columns and their schema do not permit a valid B-Tree index.");
		}
		
		// copy the parameters
		this.indexedTable = indexedTable;
		this.pageSize = pageSize;
		this.indexedColumn = column;
		this.unique = unique;
		this.rootPageNumber = rootNode;
		this.firstLeafNumber = firstLeafNode;
	}
	
	/**
	 * Gets the size of a page in this index schema.
	 * 
	 * @return The page size in bytes for this index schema.
	 */
	public PageSize getPageSize()
	{
		return this.pageSize;
	}
	
	/**
	 * Gets the number that the indexed column is in the table.
	 * 
	 * @return The column's index.
	 */
	public int getColumnNumber()
	{
		return this.indexedColumn;
	}
	
	/**
	 * Gets the schema of the table that is indexed by this index.
	 * 
	 * @return The indexed table's schema.
	 */
	public TableSchema getIndexTableSchema()
	{
		return this.indexedTable;
	}
	
	/**
	 * Gets the schema of the column in this index.
	 * 
	 * @return The schema of the column in the index.
	 */
	public ColumnSchema getIndexedColumnSchema()
	{
		return this.indexedTable.getColumn(this.indexedColumn);
	}
	
	/**
	 * Checks whether this index is a unique index.
	 * 
	 * @return true, if the index is unique, false if not.
	 */
	public boolean isUnique()
	{
		return this.unique;
	}

	/**
	 * Gets the order of the tree, i.e. the maximal number of keys in inner nodes.
	 * 
	 * @return The order of the B-Tree.
	 */
	public int getFanOut()
	{
		return this.fanOut;
	}

	/**
	 * Gets the maximal number of key / RID pairs that can be contained in the leafs.
	 * 
	 * @return The maximal number of key / RID pairs that can be contained in the leafs.
	 */
	public int getMaximalLeafEntries()
	{
		return this.maximalLeafEntries;
	}

	/**
	 * Gets the page number of the root node.
	 * 
	 * @return The page number of the root node.
	 */
	public int getRootPageNumber()
	{
		return this.rootPageNumber;
	}

	/**
	 * Sets the page number of the root node. If an IndexResourceManager
	 * is registered at this schema, it will be used to persist the changes.
	 * 
	 * @param rootPageNumber The new page number for the root page.
	 * @throws IOException Thrown in the case that a resource manager is registered to persist
	 *                     changes to the schema and the persisting fails.
	 */
	public void setRootPageNumber(int rootPageNumber)
	throws IOException
	{
		if (rootPageNumber < 1) {
			throw new IllegalArgumentException("Root page number cannot be smaller than 1");
		}
		
		this.rootPageNumber = rootPageNumber;
		
		if (this.toNotifyOnChanges != null) {
			this.toNotifyOnChanges.updateRootPageNumber(rootPageNumber);
		}
	}

	/**
	 * Gets the page number of the left-most leaf node.
	 * 
	 * @return The page number of the left-most node.
	 */
	public int getFirstLeafNumber()
	{
		return this.firstLeafNumber;
	}

	/**
	 * Sets the page number of the first leaf node. If an IndexResourceManager
	 * is registered at this schema, it will be used to persist the changes.
	 * 
	 * @param firstLeafNumber The new page number for the first leaf page.
	 * @throws IOException Thrown in the case that a resource manager is registered to persist
	 *                     changes to the schema and the persisting fails.
	 */
	public void setFirstLeafNumber(int firstLeafNumber)
	throws IOException
	{
		if (firstLeafNumber < 1) {
			throw new IllegalArgumentException("First leaf page number cannot be smaller than 1");
		}
		
		this.firstLeafNumber = firstLeafNumber;
		
		if (this.toNotifyOnChanges != null) {
			this.toNotifyOnChanges.updateFirstLeafPageNumber(firstLeafNumber);
		}
	}
	
	
	/**
	 * Sets the resource manager to be notified upon changes to the index schema.
	 * If set to null, changes will not e persisted.
	 * 
	 * @param manager The Resource manager that persists changes to the schema.
	 */
	public void setResourceManagerForPersistence(IndexResourceManager manager)
	{
		this.toNotifyOnChanges = manager;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder();
		builder.append("column ").append(this.indexedColumn).append(" (");
		builder.append(getIndexedColumnSchema());
		builder.append(" PAGE_SIZE ").append(getPageSize());
		
		if (this.unique) {
			builder.append(" UNIQUE");
		}
		builder.append(" (Fan-out: ").append(this.fanOut);
		builder.append(", Leaf Entries: ").append(this.maximalLeafEntries).append(')');
		
		return builder.toString();
	}
	
}
