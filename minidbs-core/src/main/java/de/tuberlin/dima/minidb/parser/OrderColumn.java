package de.tuberlin.dima.minidb.parser;


import java.util.Arrays;
import java.util.Iterator;


/**
 * Parse tree node representing an order column (column in the order by clause together
 * with sort order)
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OrderColumn implements ParseTreeNode
{
	/**
	 * Enumeration specifying the sort order.
	 */
	public enum Order {
		ASCENDING, DESCENDING;
	}
	
	/**
	 * The column in this order column.
	 */
	protected OutputColumn column;
	
	/**
	 * The sort order.
	 */
	protected Order order;
	
	
	/**
	 * Creates an order column for the given column alias in ascending order.
	 * 
	 * @param columnAlias The column to order.
	 */
	public OrderColumn(String columnAlias)
	{
		this(columnAlias, Order.ASCENDING);
	}

	/**
	 * Creates an order column for the given column alias with given order.
	 * 
	 * @param columnAlias The column to order.
	 * @param order  The sort order.
	 */
	public OrderColumn(String columnAlias, Order order)
	{
		this.column = new OutputColumn(null, columnAlias);
		this.order = order;
	}
	
	
	/**
	 * Creates an order column for the given column in ascending order.
	 * 
	 * @param column The column to order.
	 */
	public OrderColumn(OutputColumn column)
	{
		this(column, Order.ASCENDING);
	}

	/**
	 * Creates an order column for the given column with given order.
	 * 
	 * @param column The column to order.
	 * @param order  The sort order.
	 */
	public OrderColumn(OutputColumn column, Order order)
	{
		this.column = column;
		this.order = order;
	}
	
	

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<OutputColumn> getChildren()
	{
		return Arrays.asList(new OutputColumn[] {this.column}).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		return this.column.getNodeContents() + (this.order == Order.DESCENDING ? " DESC" : " ASC");
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "ORDER COLUMN";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return 1;
	}

	/**
	 * Gets the column in this order column.
	 * 
	 * @return The column.
	 */
	public OutputColumn getColumn()
	{
		return this.column;
	}

	/**
	 * Gets the sort order for this order column.
	 * 
	 * @return The sort order.
	 */
	public Order getOrder()
	{
		return this.order;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof OrderColumn)
		{
			OrderColumn other = (OrderColumn) node;
			
			return ( (this.column == null && other.column == null) ||
					 (this.column != null && other.column != null &&
					  this.column.isIdenticalTo(other.column))
				   ) && (this.order == other.order);
		}
		
		return false;
		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return getNodeContents();
	}
}
