package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Class representing a parsed UPDATE query. Update queries can be of the type:
 * 
 * UPDATE table SET a.b = y WHERE a.c = x
 * 
 * @author Michael Saecker
 */
public class UpdateQuery extends ParsedQuery
{
	
	/**
	 * The name of the table to perform updates on.
	 */
	protected TableReference table;
	
	/**
	 * The set clause with the predicates.
	 */
	protected SetClause set;
	
	/**
	 * The where clause with the predicates. (optional)
	 */
	protected WhereClause where;

	/**
	 * A list with all children of this query.
	 */
	private List<ParseTreeNode> allChildren;
	
	/**
	 * Creates a plain empty update query.
	 */
	public UpdateQuery()
	{
		this.allChildren = new ArrayList<ParseTreeNode>(2);
	}

	/**
	 * Gets the name of the table in this update query.
	 * 
	 * @return The table name.
	 */
	public TableReference getTable()
	{
		return this.table;
	}

	/**
	 * Sets the name of the table in this update query.
	 * 
	 * @param table The table name.
	 */
	public void setTable(TableReference table)
	{
		this.table = table;
	}

	/* 
	 * --------------------------------------------------------------------
	 *                             Clauses
	 * --------------------------------------------------------------------
	 */	
	
	/**
	 * Sets the SET clause.
	 * 
	 * @param set The SET clause.
	 */
	public void setSetClause(SetClause set)
	{
		if (this.set != null) {
			this.allChildren.remove(this.set);
		}
		
		this.set = set;
		this.allChildren.add(set);
	}
	
	/**
	 * Gets the SET clause, or null, if none is set.
	 * 
	 * @return The SET clause.
	 */
	public SetClause getSetClause() {
		return this.set;
	}
		
	/**
	 * Sets the WHERE clause.
	 * 
	 * @param where The WHERE clause.
	 */
	public void setWhereClause(WhereClause where)
	{
		if (this.where != null) {
			this.allChildren.remove(this.where);
		}
		
		this.where = where;
		this.allChildren.add(where);
	}
	
	/**
	 * Gets the WHERE clause, or null, if none is set.
	 * 
	 * @return The WHERE clause.
	 */
	public WhereClause getWhereClause() {
		return this.where;
	}	
	
	/* 
	 * --------------------------------------------------------------------
	 *                           Generic Node
	 * --------------------------------------------------------------------
	 */

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<? extends ParseTreeNode> getChildren()
	{
		return this.allChildren.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder("UPDATE ");
		bld.append(this.table);
		bld.append(' ').append(this.set.getNodeContents());
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
		return "UPDATE";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return this.allChildren.size();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof UpdateQuery) {
			UpdateQuery other = (UpdateQuery) node;

			// check table name and select clause
			return ( (this.table == null && other.table == null) ||
			         (this.table != null && other.table != null &&
			          this.table.isIdenticalTo(other.table))
			       ) && (
			         (this.set == null && other.set == null) ||
			         (this.set != null && other.set != null &&
			          this.set.isIdenticalTo(other.set))
			       ) && (
			         (this.where == null && other.where == null) ||
			         (this.where != null && other.where != null &&
			          this.where.isIdenticalTo(other.where))
			       );
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
