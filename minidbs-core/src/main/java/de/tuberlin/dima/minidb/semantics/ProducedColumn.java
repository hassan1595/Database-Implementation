package de.tuberlin.dima.minidb.semantics;


import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.parser.OutputColumn;


/**
 * This class describes a column produced by a select query.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ProducedColumn extends Column
{
	/**
	 * The aggregate function applied on the column.
	 */
	private final OutputColumn.AggregationType aggregationFunction;
	
	/**
	 * The name of the column for the result set or for reference.
	 */
	private final String columnName;
	
	/**
	 * The data type of the column output. It may be different from the data-type
	 * of the column that this output column is based on. For example if the produced
	 * column is COUNT(col1), where col1 was a CHAR(10) column, then the column's data type
	 * is CHAR(10) and the produced column's output data type is INT.
	 */
	private DataType outputDataType;
	
	/**
	 * The order for this output column, as requested in the ORDER BY clause.
	 */
	private Order order = Order.NONE;
	
	/**
	 * The order rank, i.e. the position of the column in the order by clause.
	 * The count starts at 1.
	 */
	private int orderRank;
	
	/**
	 * A reference to the original parsed column.
	 */
	private final OutputColumn parsedColumn;
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Creates a new produced column. The column comes from a relation (table or sub-query)
	 * and has an alias name (specified behind the 'AS' keyword in the SELECT clause). It
	 * is a not-aggregated column. 
	 * 
	 * @param origin The table or sub-query that the column comes from.
	 * @param type The data type of the column.
	 * @param columnIndex The index (position) of the column in the table or sub-query.
	 * @param columnAliasName The alias name of the query.
	 */
	public ProducedColumn(Relation origin, DataType type, int columnIndex,
						  String columnAliasName, OutputColumn parsedColumn)
	{
		this(origin, type, columnIndex, columnAliasName, parsedColumn, null);
		
	}
	
	/**
	 * Creates a new produced column. The column comes from a relation (table or sub-query)
	 * and has an alias name (specified behind the 'AS' keyword in the SELECT clause). It
	 * may be an aggregated column, if the aggregation function in <tt>null</tt> or <tt>NONE</tt>. 
	 * 
	 * @param origin The table or sub-query that the column comes from.
	 * @param type The data type of the column.
	 * @param columnIndex The index (position) of the column in the table or sub-query.
	 * @param columnAliasName The alias name of the query.
	 * @param aggFunction The aggregation function.
	 */
	public ProducedColumn(Relation origin, DataType type, int columnIndex,
			  String columnAliasName, OutputColumn parsedColumn, OutputColumn.AggregationType aggFunction)
	{
		super(origin, type, columnIndex);
		this.aggregationFunction = (aggFunction == null ? OutputColumn.AggregationType.NONE : aggFunction);
		this.columnName = columnAliasName;
		this.orderRank = -1;
		this.parsedColumn = parsedColumn;
	}
	
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Gets the alias name of the column.
	 * 
	 * @return The column's alias name.
	 */
	public String getColumnAliasName()
	{
		return this.columnName;
	}
	
	/**
	 * Gets the aggregation function applied on the column.
	 * 
	 * @return The column's aggregation function, or <tt>NONE</tt>, if the column is
	 *         not aggregated.
	 */
	public OutputColumn.AggregationType getAggregationFunction()
	{
		return this.aggregationFunction;
	}
	
	/**
	 * Sets the data type of the column output. It may be different from the data-type
	 * of the column that this output column is based on. For example if the produced
	 * column is COUNT(col1), where col1 was a CHAR(10) column, then the column's data type
	 * is CHAR(10) and the produced column's output data type is INT.
	 */
	public void setOutputDataType(DataType type)
	{
		this.outputDataType = type;
	}
	
	/**
	 * Gets the output data type of the column. If the output data type is not
	 * explicitly set, it is the same as the input data type.
	 * 
	 * @return The column's output data type.
	 */
	public DataType getOutputDataType()
	{
		return this.outputDataType == null ? getDataType() : this.outputDataType;
	}

	/**
	 * Gets the order of this column. If no order has been set, it will be
	 * <tt>Order.NONE</tt>.
	 * 
	 * @return This column's order.
	 */
	public Order getOrder()
	{
		return this.order;
	}

	/**
	 * Sets the order for this column.
	 * 
	 * @param order The order for this column.
	 */
	public void setOrder(Order order)
	{
		this.order = order;
	}

	/**
	 * Gets the order rank, starting at 0. If this produced column was listed
	 * at the first position in the ORDER BY clause, it has an order rank of 1.
	 * If listed at the second position, it has a rank of 2, and so on.
	 * 
	 * @return The order rank of this column.
	 */
	public int getOrderRank()
	{
		return this.orderRank;
	}

	/**
	 * Sets the order rank for this colum.
	 * If this produced column was listed at the first position in the ORDER BY clause,
	 * it must have an order rank of 1. If listed at the second position, the rank 
	 * must be 2, and so on.
	 * 
	 * @param orderRank The order rank to set.
	 */
	public void setOrderRank(int orderRank)
	{
		this.orderRank = orderRank;
	}
	
	/**
	 * Gets the parsedColumn from this ProducedColumn.
	 * 
	 * @return The parsedColumn.
	 */
	public OutputColumn getParsedColumn()
	{
		return this.parsedColumn;
	}
}
