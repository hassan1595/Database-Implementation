package de.tuberlin.dima.minidb.catalogue;


import de.tuberlin.dima.minidb.io.index.IndexResourceManager;


/**
 * This objects describes indexes and is usually obtained from the catalogue. It describes
 * the file containing the index data, but it also gives access to the index's resource
 * manager, statistics and internally used IDs.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexDescriptor
{
	/**
	 * The name of the index.
	 */
	private String indexName;
	
	/**
	 * The name of the index file.
	 */
	private String indexFile;
	
	/**
	 * The name of the table that the index is built on.
	 */
	private String indexedTableName;
	
	/**
	 * The statistics on the index.
	 */
	private IndexStatistics statistics;
	
	/**
	 * The descriptor for the table that the index is built on. 
	 */
	private transient TableDescriptor indexedTable;
	
	/**
	 * The resource manager that allows access to the index.
	 */
	private transient IndexResourceManager resourceManager;
	
	/**
	 * The id that is internally used to identify the index.
	 */
	private transient int internalResourceId;

	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new index descriptor with the given name and table.
	 * 
	 * @param indexName The name of the index.
	 * @param indexedTableName The name of the indexed table.
	 * @param indexFileName The name of the file containing the index.
	 */
	public IndexDescriptor(String indexName, String indexedTableName, String indexFileName)
	{
		this(indexName, indexedTableName, indexFileName, null);
	}
	
	/**
	 * Creates a new index descriptor with the given name, table, and statistics.
	 * 
	 * @param indexName The name of the index.
	 * @param indexedTableName The name of the indexed table.
	 * @param indexFileName The name of the file containing the index.
	 * @param stats The statistics for the index.
	 */
	public IndexDescriptor(String indexName, String indexedTableName,
			String indexFileName, IndexStatistics stats)
	{
		this.indexName = indexName;
		this.indexFile = indexFileName;
		this.indexedTableName = indexedTableName;
		
		this.statistics = stats == null ? new IndexStatistics() : stats;
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Gets the name of the index.
	 * 
	 * @return The index name.
	 */
	public String getName()
	{
		return this.indexName;
	}
	
	/**
	 * Gets the name of the file containing the index.
	 * 
	 * @return The index file name.
	 */
	public String getFileName()
	{
		return this.indexFile;
	}
	
	/**
	 * Gets the name of the indexed table.
	 * 
	 * @return The indexed table's name.
	 */
	public String getIndexedTableName()
	{
		return this.indexedTableName;
	}
	
	/**
	 * Gets the statistics for this index.
	 * 
	 * @return This index's statistics
	 */
	public IndexStatistics getStatistics()
	{
		return this.statistics;
	}
	
	/**
	 * Gets the internal resource id of this index.
	 * 
	 * @return The resource id.
	 */
	public int getResourceId()
	{
		return this.internalResourceId;
	}
	
	/**
	 * Gets the resource manager for this index.
	 * 
	 * @return This index's resource manager.
	 */
	public IndexResourceManager getResourceManager()
	{
		return this.resourceManager;
	}
	
	/**
	 * Gets the index schema.
	 * 
	 * @return The index schema.
	 */
	public IndexSchema getSchema()
	{
		return this.resourceManager == null ? null : this.resourceManager.getSchema();
	}
	
	
	/**
	 * Sets the resource properties that are only available after the files have been opened.
	 * 
	 * @param resourceManager The index's resource manager.
	 * @param indexedTable The descriptor of the indexed table.
	 * @param resourceId The internal resource id.
	 */
	public void setResourceProperties(IndexResourceManager resourceManager, 
			                          TableDescriptor indexedTable, int resourceId)
	{
		// parameter check
		if (resourceManager == null) {
			throw new NullPointerException("ResourceManager must not be null.");
		}
		if (indexedTable == null) {
			throw new NullPointerException("IndexedTable must not be null");
		}
		
		// assign fields
		if (this.resourceManager == null) {
			// not been set before, set now
			this.resourceManager = resourceManager;
			this.indexedTable = indexedTable;
			this.internalResourceId = resourceId;
		}
		else if (resourceManager != this.resourceManager ||
				this.indexedTable != indexedTable ||
				this.internalResourceId != resourceId)
		{
			// has been set previously, must not be assigned to a new value
			throw new IllegalStateException(
					"Resource peroperties have previously been assigned to different" +
					" values and must not be reassigned");
		}
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder("Index '");
		builder.append(this.indexName);
		builder.append("' on table '").append(this.indexedTableName);
		builder.append("' (").append(this.statistics).append(") (ID: ");
		builder.append(this.internalResourceId).append(", File: ").append(this.indexFile);
		builder.append(')');
		
		return builder.toString();
	}

}
