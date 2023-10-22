package de.tuberlin.dima.minidb.catalogue.beans;


/**
 * Simple bean like container for the persistent subset of the table descriptor.
 * Used during catalogue serialization and deserialization.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableDescriptorBean
{
	/**
	 * The name of the table.
	 */
	private String tableName;
	
	/**
	 * The name of the file that contains the table data.
	 */
	private String tableFileName;
	
	/**
	 * The bean containing the statistics about this table.
	 */
	private TableStatisticsBean statistics;
	
	
	
	/**
	 * Creates a plain table descriptor bean.
	 */
	public TableDescriptorBean() {
	}



	/**
     * Gets the tableName from this TableDescriptorBean.
     *
     * @return The tableName.
     */
    public String getTableName()
    {
    	return this.tableName;
    }



	/**
     * Gets the tableFileName from this TableDescriptorBean.
     *
     * @return The tableFileName.
     */
    public String getTableFileName()
    {
    	return this.tableFileName;
    }

	/**
     * Gets the statistics from this TableDescriptorBean.
     *
     * @return The statistics.
     */
    public TableStatisticsBean getStatistics()
    {
    	return this.statistics;
    }

	/**
     * Sets the tableName for this TableDescriptorBean.
     *
     * @param tableName The tableName to set.
     */
    public void setTableName(String tableName)
    {
    	this.tableName = tableName;
    }

	/**
     * Sets the tableFileName for this TableDescriptorBean.
     *
     * @param tableFileName The tableFileName to set.
     */
    public void setTableFileName(String tableFileName)
    {
    	this.tableFileName = tableFileName;
    }

	/**
     * Sets the statistics for this TableDescriptorBean.
     *
     * @param statistics The statistics to set.
     */
    public void setStatistics(TableStatisticsBean statistics)
    {
    	this.statistics = statistics;
    }
}