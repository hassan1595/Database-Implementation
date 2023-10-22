package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Abstract super class of all parse tree nodes that are clauses with lists
 * of elements (columns, predicates, ...). A simplification for the implementation
 * of many other nodes. Not to be used directly.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class AbstractListNode<T extends ParseTreeNode> implements ParseTreeNode
{
	
	/**
	 * The name of the parse tree node.
	 */
	private String nodeName;
	
	/**
	 * The delimiter in the list of elements.
	 */
	private String listDelimiter;
	
	/**
	 * The list of elements in this node.
	 */
	private List<T> elements;
	
	/**
	 * Whether the order in the list is important.
	 */
	private boolean orderDependent;
	
	/**
	 * Creates an empty node with given name.
	 * 
	 * @param name The node name.
	 * @param orderDependent Whether the order in the list is important.
	 */
	public AbstractListNode(String name, boolean orderDependent)
	{
		this (name, " ", orderDependent);
	}
	
	/**
	 * Creates an empty node with given name and delimiter. The delimiter is used to compose
	 * the node contents, which is the contents of the children, separated by the delimiter.
	 * 
	 *  @param name The node name.
	 *  @param delimiter The list delimiter.
	 *  @param orderDependent Whether the order in the list is important.
	 */
	public AbstractListNode(String name, String delimiter, boolean orderDependent)
	{
		this.nodeName = name;
		this.listDelimiter = delimiter;
		this.elements = new ArrayList<T>();
		this.orderDependent = orderDependent;
	}
	
	
	/**
	 * Add an element to the list of the node.
	 * 
	 * @param e The element to add.
	 */
	protected void addElement(T e)
	{
		if (e == null) {
			throw new IllegalArgumentException("Must not add a null element to the " +
					this.nodeName + " clause");
		}
		this.elements.add(e);
	}
	
	/**
	 * Gets the element in the node list at the given position.
	 * 
	 * @param index The position of the element to get.
	 * @return The retrieved element.
	 */
	protected T getElement(int index)
	{
		if (index < 0 || index >= this.elements.size()) {
			throw new IllegalArgumentException("Element index " + index + " is out of bounds.");
		}
		return this.elements.get(index);
	}
	
	/**
	 * Sets the element at the given position in the node list to the given element.
	 * 
	 * @param element The new element for the position.
	 * @param index The position of the element to get.
	 */
	protected void setElement(T element, int index)
	{
		if (index < 0 || index >= this.elements.size()) {
			throw new IllegalArgumentException("Element index " + index + " is out of bounds.");
		}
		this.elements.set(index, element);
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<T> getChildren()
	{
		return this.elements.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder(this.nodeName).append(' ');
		
		for (int i = 0; i < this.elements.size(); i++) {
			bld.append(this.elements.get(i).getNodeContents());
			if (i != this.elements.size() - 1) {
				bld.append(this.listDelimiter);
			}
		}
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return this.nodeName;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return this.elements.size();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return getNodeContents();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdentical(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof AbstractListNode)
		{
			AbstractListNode<?> ln = (AbstractListNode<?>) node;
			
			if (this.elements.size() == ln.elements.size()) {
				if (this.orderDependent)
				{
					for (int i = 0; i < this.elements.size(); i++) {
						if (!(this.elements.get(i).isIdenticalTo(ln.elements.get(i)))) {
							return false;
						}
					}
					
					return true;
				}
				else {
					outer: for (int i = 0; i < this.elements.size(); i++) {
						ParseTreeNode e = this.elements.get(i);
						for (int k = 0; k < ln.elements.size(); k++) {
							ParseTreeNode v = ln.elements.get(i);
							if (e.isIdenticalTo(v)) {
								continue outer;
							}
						}
						// we get here only, if we have not found a node from the outer
						// in the inner loop
						return false;
					}
					
					return true;
				}
			}
		}
		
		return false;
	}
}
