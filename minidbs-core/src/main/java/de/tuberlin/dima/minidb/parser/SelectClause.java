package de.tuberlin.dima.minidb.parser;


/**
 * Implementation of a parse tree node that represents a select clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SelectClause extends AbstractListNode<OutputColumn>
{
	/**
	 * Default constructor to create an empty select clause.
	 */
	public SelectClause()
	{
		super("SELECT", ", ", true);
	}
	
	
	/**
	 * Adds an <code>OutputColumn</code> to the list of output columns in this SELECT clause.
	 * 
	 * @param oCol The column to add.
	 */
	public void addOutputColumn(OutputColumn oCol)
	{
		super.addElement(oCol);
	}
	
	/**
	 * Gets the column at the given index.
	 * 
	 * @param index The index of the column to get.
	 * @return The requested column.
	 */
	public OutputColumn getOutputColumn(int index)
	{
		return super.getElement(index);
	}
	
	/**
	 * Sets the column at given position to the given column.
	 * 
	 * @param col The column to set.
	 * @param index The position to set the column.
	 */
	public void setOutputColumn(OutputColumn col, int index)
	{
		super.setElement(col, index);
	}
}
