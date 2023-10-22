package de.tuberlin.dima.minidb.catalogue;


import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.catalogue.beans.ColumnStatisticsBean;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataType;


/**
 * A simple class that describes the statistics of a column.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ColumnStatistics
{
	/**
	 * The highest value in the column. 
	 */
	private DataField highKey;
	
	/**
	 * The lowest value in the column. 
	 */
	private DataField lowKey;
	
	/**
	 * The cardinality (= number of distinct values) in the column.
	 */
	private long cardinality;
	
	
	/**
	 * Creates a new ColumnStatistics with the default values.
	 * 
	 * @param columnType The data type of the column.
	 */
	protected ColumnStatistics(DataType columnType)
	{
		this.cardinality = Constants.DEFAULT_COLUMN_CARDINALITY;
		this.highKey = columnType.getNullValue();
		this.lowKey = columnType.getNullValue();
	}

	/**
	 * Creates a new ColumnStatistics based on the given data type and the raw
	 * statistics bean.
	 * 
	 * @param columnType The data type of the column.
	 * @param bean The raw statistics.
	 * @throws DataFormatException Thrown, if the strings as contained in the bean
	 *         did not describe a valid value for the given data type.
	 */
	protected ColumnStatistics(DataType columnType, ColumnStatisticsBean bean)
	throws DataFormatException
	{
		if (bean == null) {
			this.cardinality = Constants.DEFAULT_COLUMN_CARDINALITY;
			this.highKey = columnType.getNullValue();
			this.lowKey = columnType.getNullValue();
		}
		else {
			this.cardinality = bean.getCardinality();
			this.highKey = columnType.getFromString(bean.getHighKeyString());
			this.lowKey = columnType.getFromString(bean.getLowKeyString());
			
			if (this.highKey.compareTo(this.lowKey) < 0) {
				// invalid statistics, fall back to default
				this.highKey = columnType.getNullValue();
				this.lowKey = columnType.getNullValue();

			}
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
	 * Gets the highKey from this ColumnStatistics.
	 *
	 * @return The highKey.
	 */
	public DataField getHighKey()
	{
		return this.highKey;
	}

	/**
	 * Gets the lowKey from this ColumnStatistics.
	 *
	 * @return The lowKey.
	 */
	public DataField getLowKey()
	{
		return this.lowKey;
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
	 * Sets the highKey for this ColumnStatistics.
	 *
	 * @param highKey The highKey to set.
	 */
	public void setHighKey(DataField highKey)
	{
		this.highKey = highKey;
	}

	/**
	 * Sets the lowKey for this ColumnStatistics.
	 *
	 * @param lowKey The lowKey to set.
	 */
	public void setLowKey(DataField lowKey)
	{
		this.lowKey = lowKey;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return "Cardinality: " + this.cardinality + ", High-Key: " + this.highKey.encodeAsString() +
			", Low-Key: " + this.lowKey.encodeAsString();
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Creates a new ColumnStatistics with the default values.
	 * 
	 * @param columnType The data type of the column.
	 * @return A new ColumnStatistics object.
	 */
	public static ColumnStatistics createColumnStatistics(DataType columnType)
	{
		return new ColumnStatistics(columnType);
	}
	
	
	/**
	 * Creates a new ColumnStatistics based on the given data type and the raw
	 * statistics bean.
	 * 
	 * @param columnType The data type of the column.
	 * @param bean The raw statistics.
	 * @return A new ColumnStatistics object.
	 * @throws DataFormatException Thrown, if the strings as contained in the bean
	 *         did not describe a valid value for the given data type.
	 */
	public static ColumnStatistics createColumnStatistics(
			DataType columnType, ColumnStatisticsBean bean)
	throws DataFormatException
	{
		return new ColumnStatistics(columnType, bean);
	}
}
