package de.tuberlin.dima.minidb.parser;


/**
 * A parse tree node representing a HAVING clause of an SQL statement.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HavingClause extends AbstractListNode<Predicate>
{
	/**
	 * Creates a plain empty HAVING clause
	 */
	public HavingClause()
	{
		super("HAVING", " AND ", false);
	}
	
	
	/**
	 * Adds a predicate to the list of the HAVING clause.
	 * 
	 * @param pred The predicate to add.
	 */
	public void addPredicate(Predicate pred)
	{
		super.addElement(pred);
	}
	
	/**
	 * Gets the predicate in the HAVING clause at the given position.
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
