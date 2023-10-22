package de.tuberlin.dima.minidb.parser;


/**
 * A parse tree node representing an ORDER BY clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OrderByClause extends AbstractListNode<OrderColumn>
{
	/**
	 * Creates a plain empty ORDER BY clause.
	 */
	public OrderByClause()
	{
		super("ORDER BY", ", ", true);
	}
	
	
	/**
	 * Adds a column to the list of the ORDER BY clause.
	 * 
	 * @param col The column to add.
	 */
	public void addOrderColumn(OrderColumn col)
	{
		super.addElement(col);
	}
	
	/**
	 * Sets the column at given position to the given column.
	 * 
	 * @param col The column to set.
	 * @param index The position to set the column.
	 */
	public void setOrderColumn(OrderColumn col, int index)
	{
		super.setElement(col, index);
	}
	
	/**
	 * Gets the column in the ORDER BY clause at the given position.
	 * 
	 * @param index The position of the column to get.
	 * @return The retrieved column.
	 */
	public OrderColumn getOrderColumn(int index)
	{
		return super.getElement(index);
	}
}
