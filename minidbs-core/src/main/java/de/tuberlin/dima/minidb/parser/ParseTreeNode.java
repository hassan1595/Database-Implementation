package de.tuberlin.dima.minidb.parser;


import java.util.Iterator;


/**
 * Abstract node of an SQL parse tree, as created by the <code>SQLParser</code>. This
 * is the root in the class hierarchy of the parse tree nodes.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ParseTreeNode
{
	
	/**
	 * Returns the name (=type) of the node. E.g. the node of a WHERE clause
	 * returns 'WHERE'.
	 * 
	 * @return The node name.
	 */
	public String getNodeName();
	
	/**
	 * The node contents, such as column name, or literal.
	 * 
	 * @return The contents of the node.
	 */
	public String getNodeContents();
	
	/**
	 * Gets all children of this node.
	 * 
	 * @return An iterator over all children.
	 */
	public Iterator<? extends ParseTreeNode> getChildren();
	
	/**
	 * Counts the number of children in this node.
	 * 
	 * @return The number of children.
	 */
	public int getNumberOfChildren();
	
	
	/**
	 * Checks if this node is semantically identical to the other node.
	 * 
	 * @param node The node to be compared to.
	 * @return true, if the nodes are semantically identical, false otherwise.
	 */
	public boolean isIdenticalTo(ParseTreeNode node);

}
