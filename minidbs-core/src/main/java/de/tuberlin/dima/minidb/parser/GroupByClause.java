package de.tuberlin.dima.minidb.parser;


/**
 * A parse tree node representing a GROUP BY clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class GroupByClause extends AbstractListNode<Column>
{
	/**
	 * Creates a plain empty GROUP BY node.
	 */
	public GroupByClause()
	{
		super("GROUP BY", ", ", false);
	}
	
	/**
	 * Adds a column to the list of the GROUP BY clause.
	 * 
	 * @param col The column to add.
	 */
	public void addColumn(Column col)
	{
		super.addElement(col);
	}
	
	/**
	 * Gets the column in the GROUP BY clause at the given position.
	 * 
	 * @param index The position of the column to get.
	 * @return The retrieved column.
	 */
	public Column getColumn(int index)
	{
		return super.getElement(index);
	}
	
	/**
	 * Sets the column at given position to the given column.
	 * 
	 * @param col The column to set.
	 * @param index The position to set the column.
	 */
	public void setColumn(Column col, int index)
	{
		super.setElement(col, index);
	}
}
