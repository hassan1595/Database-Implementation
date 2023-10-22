package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Parse tree node that represents a delete clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class DeleteQuery extends ParsedQuery
{
	
	/**
	 * The name of the table that we delete from.
	 */
	protected TableReference table;
	
	/**
	 * The where clause with the predicates.
	 */
	protected WhereClause where;

	
	/**
	 * Creates a plain empty delete clause.
	 */
	public DeleteQuery()
	{
	}
	

	/**
	 * Gets the table to delete from.
	 * 
	 * @return The table to delete from.
	 */
	public TableReference getTable()
	{
		return this.table;
	}

	/**
	 * Sets the table to delete from.
	 * 
	 * @param table The table to delete from.
	 */
	public void setTable(TableReference table)
	{
		this.table = table;
	}

	/**
	 * Gets the WHERE clause with the predicates.
	 * 
	 * @return The WHERE clause with the predicates.
	 */
	public WhereClause getWhere()
	{
		return this.where;
	}

	/**
	 * Sets the WHERE clause with the predicates.
	 * 
	 * @param where The WHERE clause with the predicates.
	 */
	public void setWhere(WhereClause where)
	{
		this.where = where;
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<? extends ParseTreeNode> getChildren()
	{
		List<ParseTreeNode> nodes = new ArrayList<ParseTreeNode>();
		if (this.table != null) {
			nodes.add(this.table);
		}
		if (this.where != null) {
			nodes.add(this.where);
		}
		
		return nodes.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder("DELETE FROM ");
		bld.append(this.table);
		if (this.where != null) {
			bld.append(' ').append(this.where.getNodeContents());
		}
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "DELETE FROM";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return (this.table == null ? 0 : 1) + (this.where == null ? 0 : 1);
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node instanceof DeleteQuery)
		{
			DeleteQuery del = (DeleteQuery) node;
			return ( (this.table == null && del.table == null) ||
					 (this.table != null && del.table != null &&
					  this.table.isIdenticalTo(del.table)))
				&&
				( (this.where == null && del.where == null) ||
				  (this.where != null && del.where != null && 
				   this.where.isIdenticalTo(del.where)));
		}
		
		return false;
	}
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return getNodeContents();
	}

}
