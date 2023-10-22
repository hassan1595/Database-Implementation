package de.tuberlin.dima.minidb.parser;


/**
 * A parse tree node that represents the SET clause with a list of predicates.
 * 
 * @author Michael Saecker
 */
public class SetClause extends AbstractListNode<Predicate>
{
	/**
	 * Creates an empty SET clause node.
	 */
	public SetClause()
	{
		super("SET", ", ", false);
	}

	/**
	 * Add a predicate to the list of the SET clause.
	 * 
	 * @param ref The predicate to add.
	 */
	public void addEqualityPredicate(Predicate ref)
	{
		super.addElement(ref);
	}

	/**
	 * Gets the predicate in the SET clause at the given position.
	 * 
	 * @param index The position of the predicate to get.
	 * @return The retrieved predicate.
	 */
	public Predicate getEqualityPredicate(int index)
	{
		return super.getElement(index);
	}
	
	/**
	 * Sets the predicate at given position to the given predicate.
	 * 
	 * @param tab The predicate to set.
	 * @param index The position to set the predicate.
	 */
	public void setEqualityPredicate(Predicate tab, int index)
	{
		super.setElement(tab, index);
	}
}
