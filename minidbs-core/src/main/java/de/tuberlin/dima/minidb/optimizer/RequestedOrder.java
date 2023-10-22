package de.tuberlin.dima.minidb.optimizer;

import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Order;


/**
 * This class represents an order that is requested by an operator. It consists
 * of a column and an order (ascending / descending)
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class RequestedOrder
{
	/**
	 * The column in this order.
	 */
	private final Column column;
	
	/**
	 * The order direction.
	 */
	private final Order order;

	
	/**
	 * Creates a new requested order for the given column and direction.
	 * 
	 * @param column The column in this order.
	 * @param order Flag for the direction: True is ascending, false is descending.
	 */
	public RequestedOrder(Column column, Order order)
	{
		if (column == null) {
			throw new NullPointerException();
		}
		
		this.column = column;
		this.order = order;
	}

	
	/**
	 * Gets the column from this RequestedOrder.
	 *
	 * @return The column.
	 */
	public Column getColumn()
	{
		return this.column;
	}

	/**
	 * Gets the direction from this RequestedOrder.
	 *
	 * @return The order direction.
	 */
	public Order getOrder()
	{
		return this.order;
	}
	
	/**
	 * Checks, if this requested order on an individual column is met by the given output
	 * order.
	 *  
	 * @param outOrder The given output order.
	 * @return True, if the output order matches this requested order, false otherwise.
	 */
	public boolean isMetBy(OrderedColumn outOrder)
	{		
		// check if the direction matches check if the given output order contains out column
		return outOrder.containsColumn(this.column) && 
			this.order == outOrder.getOrder();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.column + (this.order == Order.ASCENDING ? " (asc)" : " (desc)");
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (this == o) {
			return true;
		}
		
		if (o instanceof RequestedOrder) {
			RequestedOrder other = (RequestedOrder) o;
			return this.column.equals(other.column) && this.order == other.order;
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.column.hashCode() ^ this.order.hashCode();
	}
}
