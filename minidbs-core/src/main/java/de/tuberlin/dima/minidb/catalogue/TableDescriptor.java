package de.tuberlin.dima.minidb.catalogue;


import de.tuberlin.dima.minidb.catalogue.beans.TableStatisticsBean;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;


/**
 * This objects describes tables and is usually obtained from the catalogue. It describes
 * the file containing the table data, but it also gives access to the table's resource
 * manager, statistics and internally used IDs.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableDescriptor
{
	/**
	 * The name of the table that this object describes.
	 */
	private String tableName;
	
	/**
	 * The path to the file, containing the data of this table.
	 */
	private String tableFile;
	
	/**
	 * The statistics known for this table.
	 */
	private TableStatistics statistics;
	
	/**
	 * The statistics in raw format before being interpreted through schema.
	 */
	private TableStatisticsBean rawStatistics;
	
	/**
	 * The handle for this table's resource. 
	 */
	private TableResourceManager resourceManager;
	
	/**
	 * The internally used id to reference the table.
	 */
	private int internalResourceId;

	
	
	/**
	 * Creates a new table descriptor, containing initially only the persistent
	 * fields for the table's name and the path to its file.
	 * 
	 * @param tableName The name of the table.
	 * @param tableFileName The path to the table file.
	 */
	public TableDescriptor(String tableName, String tableFileName)
	{
		this (tableName, tableFileName, null);
	}
	
	
	/**
	 * Creates a new table descriptor, containing initially only the persistent
	 * fields for the table's name and the path to its file, as well as the
	 * statistics known about this table.
	 * 
	 * The statistics are still in a text only format, not usable until the
	 * schema is known, which is when the resource properties are initialized.
	 * 
	 * @param tableName The name of the table.
	 * @param tableFileName The path to the table file.
	 * @param rawStats The raw statistics about the table.
	 */
	public TableDescriptor(String tableName, String tableFileName, TableStatisticsBean rawStats)
	{
		this.tableName = tableName;
		this.tableFile = tableFileName;
		
		this.rawStatistics = rawStats == null ? new TableStatisticsBean() : rawStats;
	}
	
	
	/**
	 * Gets the table name from this TableDescriptor.
	 *
	 * @return The table name.
	 */
	public String getTableName()
	{
		return this.tableName;
	}

	/**
	 * Gets the table file path from this TableDescriptor.
	 *
	 * @return The table file path.
	 */
	public String getFileName()
	{
		return this.tableFile;
	}

	/**
	 * Gets the statistics from this TableDescriptor.
	 *
	 * @return The statistics.
	 */
	public TableStatistics getStatistics()
	{
		return this.statistics;
	}

	/**
	 * Gets the resource manager from this TableDescriptor.
	 * This method returns null, if the resource properties have not
	 * yet been assigned.
	 *
	 * @return The resource manager.
	 */
	public TableResourceManager getResourceManager()
	{
		return this.resourceManager;
	}

	/**
	 * Gets the internalResourceId from this TableDescriptor.
	 * This method returns <i>0</i>, if the resource properties have not
	 * yet been assigned.
	 *
	 * @return The internalResourceId.
	 */
	public int getResourceId()
	{
		return this.internalResourceId;
	}

	/**
	 * Gets this table's schema. This method returns null, if the resource properties have not
	 * yet been assigned.
	 * 
	 * @return The schema of the table.
	 */
	public TableSchema getSchema()
	{
		return this.resourceManager == null ? null : this.resourceManager.getSchema();
	}
	
	
	/**
	 * Assigns this TableDescriptor the transient properties that are different each time
	 * the table is opened during startup.
	 * 
	 * @param resourceManager The resource manager for this table.
	 * @param resourceId The ID used internally to reference this table.
	 */
	public void setResourceProperties(TableResourceManager resourceManager, int resourceId)
	{
		// parameter check
		if (resourceManager == null) {
			throw new NullPointerException("ResourceManager must not be null.");
		}
		
		// assign fields
		if (this.resourceManager == null) {
			// not been set before, set now
			this.resourceManager = resourceManager;
			this.internalResourceId = resourceId;
		}
		else if (resourceManager != this.resourceManager || this.internalResourceId != resourceId)
		{
			// has been set previously, must not be assigned to a new value
			throw new IllegalStateException(
					"Resource peroperties have previously been assigned to different" +
					" values and must not be reassigned");
		}
		
		// make the statistics usable
		this.statistics = new TableStatistics(resourceManager.getSchema(), this.rawStatistics);
		this.rawStatistics = null;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder builder = new StringBuilder("Table '");
		builder.append(this.tableName);
		builder.append("' (").append(this.statistics).append(") (ID: ");
		builder.append(this.internalResourceId).append(", File: ").append(this.tableFile);
		builder.append(')');
		
		return builder.toString();
	}
}
