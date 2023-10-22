package de.tuberlin.dima.minidb.optimizer;


import java.util.Arrays;


/**
 * This class represents an order on several columns that operators are interested in.
 * An example is a Merge Join operator, which is interested in an order on its join keys,
 * or a Group By operator that is interested in an order on its grouping keys.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class InterestingOrder
{
	/**
	 * The individual column orders in this interesting orders.
	 */
	private final RequestedOrder[] orderColumns;
	
	/**
	 * The maximal additional cost that a plan that fulfills this order
	 * may have over the cheapest plan.
	 */
	private final long maxCost;
	
	
	/**
	 * Creates a new interesting order, for a set of columns (with direction) and
	 * a maximal cost delta compared to the cheapest plan. 
	 * 
	 * @param orderColumns The individual column orders in this interesting orders.
	 * @param maxCost The maximal additional cost that a plan that fulfills this order
	 *                may have over the cheapest plan.
	 */
	public InterestingOrder(RequestedOrder[] orderColumns, long maxCost)
	{
		this.orderColumns = orderColumns;
		this.maxCost = maxCost;
	}

	/**
	 * Gets the columns from this InterestingOrder.
	 *
	 * @return The columns.
	 */
	public RequestedOrder[] getOrderColumns()
	{
		return this.orderColumns;
	}

	/**
	 * Gets the maxCost from this InterestingOrder.
	 *
	 * @return The maxCost.
	 */
	public long getMaxCost()
	{
		return this.maxCost;
	}
	
	/**
	 * Checks, whether this interesting order is met by a provided output order.
	 * 
	 * @param order The provided output order.
	 * @return True, if this interesting order is met, false otherwise.
	 */
	public boolean isMetByOutputOrder(OrderedColumn[] order)
	{
		if (order.length < this.orderColumns.length) {
			// too few columns in that order.
			return false;
		}

		// check column by column
		for (int i = 0; i < this.orderColumns.length; i++) {
			if (!this.orderColumns[i].isMetBy(order[i])) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Checks, if one of the interesting orders is redundant with respect to another.
	 * An order is redundant, if it starts with the same columns and has
	 * a lower cost delta. It is redundant then, because every provided order
	 * that would meet that an interesting order would also meet the other and its
	 * delta cost requirement.
	 *   
	 * @param order1 The first interesting order.
	 * @param order2 The second interesting order.
	 * @return -1, if the first is redundant; 1, if the second is redundant; 0 if none is redundant.
	 */
	public static int checkRedundancy(InterestingOrder order1, InterestingOrder order2)
	{	
		int len1 = order1.orderColumns.length;
		int len2 = order2.orderColumns.length;
		
		if (len1 <= len2) {
			for (int i = 0; i < len1; i++) {
				if (!order1.orderColumns[i].equals(order2.orderColumns[i])) {
					return 0;
				}
			}
			// they match at the beginning, but order 2 has more columns.
			// if order 2 has a lower cost delta, it is redundant
			return order2.maxCost < order1.maxCost ? 1 : 0;
		}
		else {
			for (int i = 0; i < len2; i++) {
				if (!order1.orderColumns[i].equals(order2.orderColumns[i])) {
					return 0;
				}
			}
			// they match at the beginning, but order 1 has more columns.
			// if order 1 has a lower cost delta, it is redundant
			return order1.maxCost < order2.maxCost ? -1 : 0;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder();
		bld.append("Max-Cost: ").append(this.maxCost).append(" for ");
		for (int i = 0; i < this.orderColumns.length; i++) {
			bld.append('[').append(this.orderColumns[i].toString()).append(']');
			bld.append(' ');
		}
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (o == this) {
			return true;
		}
		
		if (o instanceof InterestingOrder) {
			InterestingOrder other = (InterestingOrder) o;
			return Arrays.deepEquals(this.orderColumns, other.orderColumns);
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return Arrays.deepHashCode(this.orderColumns);
	}
}
