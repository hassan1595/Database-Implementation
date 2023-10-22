package de.tuberlin.dima.minidb.parser;


/**
 * A parse tree node that represents the FROM clause with a list of tables.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FromClause extends AbstractListNode<TableReference>
{
	/**
	 * Creates an empty FROM clause node.
	 */
	public FromClause()
	{
		super("FROM", ", ", false);
	}

	/**
	 * Add a table to the list of the FROM clause.
	 * 
	 * @param ref The table to add.
	 */
	public void addTable(TableReference ref)
	{
		super.addElement(ref);
	}

	/**
	 * Gets the table in the FROM clause at the given position.
	 * 
	 * @param index The position of the table to get.
	 * @return The retrieved table.
	 */
	public TableReference getTable(int index)
	{
		return super.getElement(index);
	}
	
	/**
	 * Sets the table at given position to the given table.
	 * 
	 * @param tab The table to set.
	 * @param index The position to set the table.
	 */
	public void setTable(TableReference tab, int index)
	{
		super.setElement(tab, index);
	}
}
