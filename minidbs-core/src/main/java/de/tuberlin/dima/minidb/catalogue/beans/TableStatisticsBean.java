package de.tuberlin.dima.minidb.catalogue.beans;


import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.ColumnStatistics;
import de.tuberlin.dima.minidb.catalogue.TableStatistics;


/**
 * A simple bean that describes the statistics of a table.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableStatisticsBean
{
	/**
	 * The cardinality (= number of rows) in the table.
	 */
	private long cardinality;
	
	/**
	 * The number of data pages occupied by the table.
	 */
	private int numberOfPages;
	
	/**
	 * The column statistics. 
	 */
	private ColumnStatisticsBean[] columnStatistics;
	
	
	/**
	 * Creates a new TableStatistics bean with the default values.
	 */
	public TableStatisticsBean()
	{
		this.cardinality = Constants.DEFAULT_TABLE_CARDINALITY;
		this.numberOfPages = Constants.DEFAULT_TABLE_PAGES;
	}
	
	/**
	 * Creates a new table statistics bean that copies its values from the
	 * given TableStatistics object.
	 * 
	 * @param stats The TableStatistics object to copy the values from.
	 */
	public TableStatisticsBean(TableStatistics stats)
	{
		this.cardinality = stats.getCardinality();
		this.numberOfPages = stats.getNumberOfPages();
		
		this.columnStatistics = new ColumnStatisticsBean[stats.getNumberOfColumns()];
		for (int i = 0; i < this.columnStatistics.length; i++) {
			ColumnStatistics cs = stats.getColumnStatistics(i);
			this.columnStatistics[i] = new ColumnStatisticsBean(cs, i);
		}
	}


	/**
     * Gets the cardinality from this TableStatistics.
     *
     * @return the cardinality
     */
    public long getCardinality()
    {
    	return this.cardinality;
    }

	/**
     * Gets the numberOfPages from this TableStatistics.
     *
     * @return the numberOfPages
     */
    public int getNumberOfPages()
    {
    	return this.numberOfPages;
    }

	/**
	 * Gets the columnStatistics from this TableStatisticsBean.
	 *
	 * @return The columnStatistics.
	 */
	public ColumnStatisticsBean[] getColumnStatistics()
	{
		return this.columnStatistics;
	}

	/**
     * Sets the cardinality for this TableStatistics.
     *
     * @param cardinality the cardinality to set
     */
    public void setCardinality(long cardinality)
    {
    	this.cardinality = cardinality;
    }

	/**
     * Sets the numberOfPages for this TableStatistics.
     *
     * @param numberOfPages the numberOfPages to set
     */
    public void setNumberOfPages(int numberOfPages)
    {
    	this.numberOfPages = numberOfPages;
    }

	/**
	 * Sets the columnStatistics for this TableStatisticsBean.
	 *
	 * @param columnStatistics The columnStatistics to set.
	 */
	public void setColumnStatistics(ColumnStatisticsBean[] columnStatistics)
	{
		this.columnStatistics = columnStatistics;
	}
}
