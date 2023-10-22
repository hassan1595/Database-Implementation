package de.tuberlin.dima.minidb.catalogue.beans;


import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.ColumnStatistics;


/**
 * A simple bean that describes the statistics of a column.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ColumnStatisticsBean
{
	/**
	 * String representation of the second highest value. 
	 */
	private String highKeyString;
	
	/**
	 * String representation of the second lowest value.
	 */
	private String lowKeyString;
	
	/**
	 * The cardinality (= number of distinct values) in the column.
	 */
	private long cardinality;
	
	/**
	 * The index of the column;
	 */
	private int columnIndex;
	
	
	
	/**
	 * Creates a new ColumnStatistics bean with the default values.
	 */
	public ColumnStatisticsBean()
	{
		this(null, 0);
	}
	
	/**
	 * Creates a new ColumnStatistics bean with the default values for the
	 * given column index.
	 * 
	 * @param columnIndex The index of the column.
	 */
	public ColumnStatisticsBean(int columnIndex)
	{
		this(null, columnIndex);
	}
	
	/**
	 * Creates a new ColumnStatistics bean for the values of the given
	 * ColumnStatistics object and the given column index.
	 * 
	 * @param stats The ColumnStatistics object.
	 * @param columnIndex The index of the column.
	 */
	public ColumnStatisticsBean(ColumnStatistics stats, int columnIndex)
	{
		this.columnIndex = columnIndex;
		
		if (stats == null) {
			this.cardinality = Constants.DEFAULT_COLUMN_CARDINALITY;
			this.highKeyString = "";
			this.highKeyString = "";
		}
		else {
			this.cardinality = stats.getCardinality();
			this.highKeyString = stats.getHighKey().encodeAsString();
			this.lowKeyString = stats.getLowKey().encodeAsString();
		}
	}

	/**
     * Gets the cardinality from this ColumnStatistics.
     *
     * @return The cardinality.
     */
    public long getCardinality()
    {
    	return this.cardinality;
    }

	/**
	 * Gets the highKeyString from this ColumnStatistics.
	 *
	 * @return The highKeyString.
	 */
	public String getHighKeyString()
	{
		return this.highKeyString;
	}

	/**
	 * Gets the lowKeyString from this ColumnStatistics.
	 *
	 * @return The lowKeyString.
	 */
	public String getLowKeyString()
	{
		return this.lowKeyString;
	}

	/**
	 * Gets the columnIndex from this ColumnStatisticsBean.
	 *
	 * @return The columnIndex.
	 */
	public int getColumnIndex()
	{
		return this.columnIndex;
	}

	/**
	 * Sets the cardinality for this ColumnStatistics.
	 *
	 * @param cardinality The cardinality to set.
	 */
	public void setCardinality(long cardinality)
	{
		this.cardinality = cardinality;
	}

	/**
	 * Sets the highKeyString for this ColumnStatistics.
	 *
	 * @param highKeyString The highKeyString to set.
	 */
	public void setHighKeyString(String highKeyString)
	{
		this.highKeyString = highKeyString;
	}

	/**
	 * Sets the lowKeyString for this ColumnStatistics.
	 *
	 * @param lowKeyString The low2keyString to set.
	 */
	public void setLowKeyString(String lowKeyString)
	{
		this.lowKeyString = lowKeyString;
	}

	/**
	 * Sets the columnIndex for this ColumnStatisticsBean.
	 *
	 * @param columnIndex The columnIndex to set.
	 */
	public void setColumnIndex(int columnIndex)
	{
		this.columnIndex = columnIndex;
	}
}
