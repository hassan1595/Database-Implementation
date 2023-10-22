package de.tuberlin.dima.minidb.catalogue;


import de.tuberlin.dima.minidb.catalogue.beans.ColumnStatisticsBean;
import de.tuberlin.dima.minidb.catalogue.beans.TableStatisticsBean;
import de.tuberlin.dima.minidb.core.DataFormatException;


/**
 * A simple bean that describes the statistics of a table.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableStatistics
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
	 * The statistics about each column. 
	 */
	private ColumnStatistics[] columnStatictics;
	
	
	/**
	 * Creates a new TableStatistics from the values of the given bean,
	 * interpreted with the help of the given schema.
	 */
	public TableStatistics(TableSchema schema, TableStatisticsBean statsBean)
	{
		this.cardinality = statsBean.getCardinality();
		this.numberOfPages = statsBean.getNumberOfPages();
		
		// create the column statistics array
		ColumnStatisticsBean[] cols = statsBean.getColumnStatistics();
		this.columnStatictics = new ColumnStatistics[schema.getNumberOfColumns()];
		
		// fill in column statistics that were read
		if (cols != null) {
			for (int i = 0; i < cols.length; i++) {
				int index = cols[i].getColumnIndex();
				if (index >= 0 && index < this.columnStatictics.length) {
					try {
						this.columnStatictics[index] = ColumnStatistics.createColumnStatistics(
								schema.getColumn(index).getDataType(), cols[i]);
					}
					catch (DataFormatException dfex) {}
				}
			}
		}
		
		// fill in the missing columns with default statistics
		for (int i = 0; i < this.columnStatictics.length; i++) {
			if (this.columnStatictics[i] == null) {
				this.columnStatictics[i] = ColumnStatistics.createColumnStatistics(schema.getColumn(i).getDataType());
			}
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
     * Gets the number of columns that the table has statistics for.
     * 
     * @return The number of columns.
     */
    public int getNumberOfColumns()
    {
    	return this.columnStatictics == null ? 0 : this.columnStatictics.length;
    }
    
    /**
     * Gets the column statistics for the column with the given index.
     * 
     * @param columnIndex The index of the column.
     * @return The statistics for that column.
     */
    public ColumnStatistics getColumnStatistics(int columnIndex)
    {
    	return this.columnStatictics[columnIndex];
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
     * Sets the column statistics for this object.
     * 
     * @param colStats The column statistics.
     */
    public void setColumnStatistics(ColumnStatistics[] colStats)
    {
    	this.columnStatictics = colStats;
    }
    
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
	public String toString()
    {
    	return "Cardinality: " + this.cardinality + ", Number of Pages: " + this.numberOfPages;
    }
}
