package de.tuberlin.dima.minidb.semantics;


import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;

/**
 * An edge in the graph describing the join topology of the query.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class JoinGraphEdge
{
	/**
	 * The left node that the edge is attached to.
	 */
	private Relation leftNode;
	
	/**
	 * The right node that the edge is attached to.
	 */
	private Relation rightNode;
	
	/**
	 * The join predicate that is attached to this edge.
	 */
	private JoinPredicate joinPredicate;

	
	/**
	 * Creates a new edge for the given nodes and the predicate.
	 * 
	 * @param leftNode The left node for the edge.
	 * @param rightNode The right node for the edge.
	 * @param joinPredicate The join predicate to be associated with the edge.
	 */
	public JoinGraphEdge(Relation leftNode, Relation rightNode,
			JoinPredicate joinPredicate)
	{
		this.leftNode = leftNode;
		this.rightNode = rightNode;
		this.joinPredicate = joinPredicate;
	}
	

	/**
	 * Gets the left node that the edge is attached to.
	 *
	 * @return The left node.
	 */
	public Relation getLeftNode()
	{
		return this.leftNode;
	}

	/**
	 * Sets the left node that the edge is attached to.
	 * 
	 * @param left The left node.
	 */
	public void setLeftNode(Relation left)
	{
		this.leftNode = left;
	}
	
	/**
	 * Gets the right node that the edge is attached to.
	 *
	 * @return The right node.
	 */
	public Relation getRightNode()
	{
		return this.rightNode;
	}

	/**
	 * Sets the right node that the edge is attached to.
	 * 
	 * @param right The right node.
	 */
	public void setRightNode(Relation right)
	{
		this.rightNode = right;
	}
	
	/**
	 * Gets the joinPredicate from this JoinGraphEdge.
	 *
	 * @return The joinPredicate.
	 */
	public JoinPredicate getJoinPredicate()
	{
		return this.joinPredicate;
	}

	/**
	 * Sets the joinPredicate for this JoinGraphEdge.
	 *
	 * @param joinPredicate The joinPredicate to set.
	 */
	public void setJoinPredicate(JoinPredicate joinPredicate)
	{
		this.joinPredicate = joinPredicate;
	}	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(100);
		bld.append('[').append(this.leftNode).append(']').append(' ');
		bld.append(this.joinPredicate).append(' ');
		bld.append('[').append(this.rightNode).append(']');
		return bld.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o)
	{
		if (o != null && o instanceof JoinGraphEdge) {
			JoinGraphEdge e = (JoinGraphEdge) o;
			return this.leftNode == e.leftNode && this.rightNode == e.rightNode;
		}
		
		return false;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		int code1 = this.leftNode == null ? 0 : this.leftNode.hashCode();
		int code2 = this.rightNode == null ? 0 : this.rightNode.hashCode();
		return code1 ^ code2;
	}
}
