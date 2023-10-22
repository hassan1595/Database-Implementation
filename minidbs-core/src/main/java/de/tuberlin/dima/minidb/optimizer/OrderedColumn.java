package de.tuberlin.dima.minidb.optimizer;

import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.Order;


/**
 * This class represents an order on a column that is produced by a plan operator.
 * The order can either contain a single column that is ordered, or an array of columns,
 * that are equally ordered. The latter case happens for example after a Merge-Join:
 * The output is ordered equally after corresponding join key columns from both sides.
 * <p>
 * For an <tt>OutputColumnOrder</tt>, the order in which the individual equally ordered
 * columns are in the array does not matter.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class OrderedColumn
{
	/**
	 * The columns that are equivalently orders.
	 */
	private Column[] columns;
	
	/**
	 * Flag describing whether the columns are in ascending order.
	 */
	private Order order;

	
	/**
	 * Creates an output column order for a single column.
	 * 
	 * @param column The column for the order.
	 * @param order The order of the column
	 */
	public OrderedColumn(Column column, Order order)
	{
		this.columns = new Column[] { column };
		this.order = order;
	}
	
	/**
	 * Creates a column order that is based on two existing orders which have equivalently
	 * ordered columns.
	 *  
	 * @param order1 The first order.
	 * @param order2 The second order.
	 */
	public OrderedColumn(OrderedColumn order1, OrderedColumn order2)
	{
		int num1 = order1.columns.length;
		int num2 = order2.columns.length;
		
		this.columns = new Column[num1 + num2];
		System.arraycopy(order1.columns, 0, this.columns, 0, num1);
		System.arraycopy(order2.columns, 0, this.columns, num1, num2);
		
		this.order = order1.order;
	}
	
	
	/**
	 * Private constructor that initializes nothing.
	 */
	private OrderedColumn()
	{
	}


	/**
	 * Gets the order of the column.
	 *
	 * @return The order.
	 */
	public Order getOrder()
	{
		return this.order;
	}
	
	/**
	 * Checks if this order has only one column, not multiple that are
	 * equivalently ordered.
	 * 
	 * @return True, if this order has just one column, false otherwise.
	 */
	public boolean isSingleColumn()
	{
		return this.columns.length == 1;
	}
	
	/**
	 * Gets the columns from this order.
	 * 
	 * @return The columns.
	 */
	public Column[] getColumns()
	{
		return this.columns;
	}
	
	/**
	 * Checks, whether a column is contained in the columns in this order.
	 * 
	 * @param col The column to be checked.
	 * @return True, if the given column is part of the columns in this order,
	 *         false otherwise.
	 */
	public boolean containsColumn(Column col)
	{
		for (int i = 0; i < this.columns.length; i++) {
			if (this.columns[i].equals(col)) {
				return true;
			}
		}
		return false;
	}
	
//	/**
//	 * Checks, if this column output order is dominant or equal to the given one.
//	 * Dominant or equal means that all of the equally ordered columns from the
//	 * other order are also part of this order.
//	 * 
//	 * @param other The order to check against this one.
//	 * @return True, if this output order is dominant or equal, false otherwise.
//	 */
//	public boolean isDominantOrEqual(OrderedColumn other)
//	{
//		// make sure that this one has all columns from the other
//		// check if this one contains all columns from the other one
//		outer: for (int i = 0; i < other.columns.length; i++) {
//			Column otherCol = other.columns[i];
//			for (int k = 0; k < columns.length; k++) {
//				if (otherCol.equals(columns[k])) {
//					continue outer;
//				}
//			}
//			return false;
//		}
//		return true;
//	}
	
	/**
	 * Returns a OutputColumnOrder that corresponds to this on, but only includes the
	 * output columns that are in the given list. If none remains, this method returns
	 * <i>null</i>.
	 * 
	 * @param cols The list of columns by which to filter.
	 * @return The OutputColumnOrder with the filtered list of equally ordered columns.
	 */
	public OrderedColumn filterByColumns(Column[] cols)
	{
		Column[] newCols = new Column[this.columns.length];
		int numFound = 0;
		for (int i = 0; i < this.columns.length; i++) {
			Column toTest = this.columns[i];
			for (int k = 0; k < cols.length; k++) {
				if (toTest.equals(cols[k])) {
					newCols[numFound++] = toTest;
					break;
				}
			}
		}
		
		if (numFound == 0) {
			return null;
		}
		else if (numFound == this.columns.length) {
			return this;
		}
		else {
			OrderedColumn co = new OrderedColumn();
			co.order = this.order;
			co.columns = new Column[numFound];
			System.arraycopy(newCols, 0, co.columns, 0, numFound);
			return co;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder();
		if (isSingleColumn()) {
			bld.append(this.columns[0].toString());
		}
		else {
			bld.append('(');
			for (int i = 0; i < this.columns.length - 1; i++) {
				bld.append(this.columns[i].toString()).append(' ');
				bld.append('|').append(' ');
			}
			bld.append(this.columns[this.columns.length - 1].toString());
			bld.append(')');
		}
		
		bld.append(this.order == Order.ASCENDING ? " ASC" : " DESC");
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		// fast check
		if (this == o) {
			return true;
		}
		
		if (o != null && o instanceof OrderedColumn) {
			OrderedColumn other = (OrderedColumn) o;
			// check direction and number of columns
			if (other.order != this.order || other.columns.length != this.columns.length) {
				return false;
			}
			
			// check if this one contains all columns from the other one
			outer: for (int i = 0; i < this.columns.length; i++) {
				Column col = other.columns[i];
				for (int k = 0; k < this.columns.length; k++) {
					if (col.equals(this.columns[k])) {
						continue outer;
					}
				}
				return false;
			}
			return true;
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		// hash code involves all columns. because the order
		// of the columns in the array does not matter, we 
		// use a function that is independent of the order
		int code = 0x87a3e19c;
		for (int i = 0; i < this.columns.length; i++) {
			code ^= this.columns[i].hashCode();
		}
		return this.order == Order.ASCENDING ? code : ~code;
	}
}
