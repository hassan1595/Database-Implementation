package de.tuberlin.dima.minidb.catalogue.beans;


import de.tuberlin.dima.minidb.catalogue.IndexStatistics;


/**
 * Simple bean like container for the persistent subset of the index descriptor.
 * Used during catalogue serialization and deserialization.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexDescriptorBean
{
	/**
	 * The name of the index.
	 */
	private String indexName;
	
	/**
	 * The name of the file that contains the index data.
	 */
	private String indexFileName;
	
	/**
	 * The name of the table that is indexed.
	 */
	private String indexedTableName;
	
	/**
	 * The bean containing the statistics about this index.
	 */
	private IndexStatistics statistics;
	
	
	
	/**
	 * Creates a plain index descriptor bean.
	 */
	public IndexDescriptorBean() {
	}



	/**
     * Gets the indexName from this IndexDescriptorBean.
     *
     * @return The indexName.
     */
    public String getIndexName()
    {
    	return this.indexName;
    }

	/**
     * Gets the indexFileName from this IndexDescriptorBean.
     *
     * @return The indexFileName.
     */
    public String getIndexFileName()
    {
    	return this.indexFileName;
    }

	/**
     * Gets the indexedTableName from this IndexDescriptorBean.
     *
     * @return The indexedTableName.
     */
    public String getIndexedTableName()
    {
    	return this.indexedTableName;
    }

	/**
     * Gets the statistics from this IndexDescriptorBean.
     *
     * @return The statistics.
     */
    public IndexStatistics getStatistics()
    {
    	return this.statistics;
    }

	/**
     * Sets the indexName for this IndexDescriptorBean.
     *
     * @param indexName The indexName to set.
     */
    public void setIndexName(String indexName)
    {
    	this.indexName = indexName;
    }

	/**
     * Sets the indexFileName for this IndexDescriptorBean.
     *
     * @param indexFileName The indexFileName to set.
     */
    public void setIndexFileName(String indexFileName)
    {
    	this.indexFileName = indexFileName;
    }

	/**
     * Sets the indexedTableName for this IndexDescriptorBean.
     *
     * @param indexedTableName The indexedTableName to set.
     */
    public void setIndexedTableName(String indexedTableName)
    {
    	this.indexedTableName = indexedTableName;
    }

	/**
     * Sets the statistics for this IndexDescriptorBean.
     *
     * @param statistics The statistics to set.
     */
    public void setStatistics(IndexStatistics statistics)
    {
    	this.statistics = statistics;
    }
}
