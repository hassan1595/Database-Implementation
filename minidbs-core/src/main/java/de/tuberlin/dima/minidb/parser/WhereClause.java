package de.tuberlin.dima.minidb.parser;


public class WhereClause extends AbstractListNode<Predicate>
{
	/**
	 * Creates a plain empty WHERE clause
	 */
	public WhereClause()
	{
		super("WHERE", " AND ", false);
	}
	
	
	/**
	 * Adds a predicate to the list of the WHERE clause.
	 * 
	 * @param pred The predicate to add.
	 */
	public void addPredicate(Predicate pred)
	{
		super.addElement(pred);
	}
	
	/**
	 * Gets the predicate in the WHERE clause at the given position.
	 * 
	 * @param index The position of the predicate to get.
	 * @return The retrieved predicate.
	 */
	public Predicate getPredicate(int index)
	{
		return super.getElement(index);
	}
	
	/**
	 * Sets the predicate at given position to the given predicate.
	 * 
	 * @param pred The predicate to set.
	 * @param index The position to set the predicate.
	 */
	public void setPredicate(Predicate pred, int index)
	{
		super.setElement(pred, index);
	}
}
