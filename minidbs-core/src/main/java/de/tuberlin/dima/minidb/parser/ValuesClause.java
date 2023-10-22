package de.tuberlin.dima.minidb.parser;


/**
 * A class representing a VALUES clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ValuesClause extends AbstractListNode<Literal>
{
	
	/**
	 * Creates a plain empty values clause.
	 */
	public ValuesClause()
	{
		super("VALUES", "", true);
	}

	/**
	 * Adds a value to this list.
	 * 
	 * @param value The value to add.
	 */
	public void addValue(Literal value)
	{
		super.addElement(value);
	}

	/**
	 * Gets the value at the given position.
	 * 
	 * @param index The index of the value to retrieve.
	 * @return The value at the given index.
	 */
	public Literal getValue(int index)
	{
		return super.getElement(index);
	}

	/**
	 * Sets the value at the given position to the given new value.
	 * 
	 * @param value The new value.
	 * @param index The position for the new value.
	 */
	public void setValue(Literal value, int index)
	{
		super.setElement(value, index);
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.AbstractListNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < getNumberOfChildren(); i++)
		{
			Literal l = getValue(i);
			if (l != null) {
				bld.append(l.getNodeContents());
			} else {
				bld.append("NULL");
			}
			
			if (i < getNumberOfChildren() - 1) {
				bld.append(", ");
			}
		}
		
		bld.append(')');
		
		return bld.toString();
	}
}
