package de.tuberlin.dima.minidb.parser;


import java.util.Collections;
import java.util.Iterator;


/**
 * The abstract superclass of all literals (string, integer, real).
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class Literal implements ParseTreeNode
{

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<? extends ParseTreeNode> getChildren()
	{
		return Collections.<ParseTreeNode>emptyList().iterator();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return getNodeContents();
	}
	
	/**
	 * Gets the raw literal value, without any quotations or any
	 * extra leading or training characters.
	 * 
	 * @return The literal value.
	 */
	public abstract String getLiteralValue();
}
