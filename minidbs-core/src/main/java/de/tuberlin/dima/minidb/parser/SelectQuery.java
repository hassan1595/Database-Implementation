package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * The node in the parse tree that represents the entire parsed select query.
 *  
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SelectQuery extends ParsedQuery
{
	
	/**
	 * The select clause in this query.
	 */
	protected SelectClause select;

	/**
	 * The select clause in this query.
	 */
	protected FromClause from;
	
	/**
	 * The select clause in this query.
	 */
	protected WhereClause where;
	
	/**
	 * The select clause in this query.
	 */
	protected GroupByClause groupBy;
	
	/**
	 * The select clause in this query.
	 */
	protected HavingClause having;
	
	/**
	 * The select clause in this query.
	 */
	protected OrderByClause orderBy;
	
	
	/**
	 * A list with all children of this query.
	 */
	private List<ParseTreeNode> allChildren;
	

	/* 
	 * --------------------------------------------------------------------
	 *                           Constructors
	 * --------------------------------------------------------------------
	 */
	
	/**
	 * Creates an empty, not described query node.
	 */
	public SelectQuery()
	{
		this.allChildren = new ArrayList<ParseTreeNode>(6);
	}
	
	
	/* 
	 * --------------------------------------------------------------------
	 *                             Clauses
	 * --------------------------------------------------------------------
	 */
	
	
	/**
	 * Sets the SELECT clause.
	 * 
	 * @param select The SELECT clause.
	 */
	public void setSelectClause(SelectClause select)
	{
		if (this.select != null) {
			this.allChildren.remove(this.select);
		}
		
		this.select = select;
		this.allChildren.add(select);
	}
	
	/**
	 * Gets the SELECT clause, or null, if none is set.
	 * 
	 * @return The SELECT clause.
	 */
	public SelectClause getSelectClause() {
		return this.select;
	}
	
	/**
	 * Sets the FROM clause.
	 * 
	 * @param from The FROM clause.
	 */
	public void setFromClause(FromClause from)
	{
		if (this.from != null) {
			this.allChildren.remove(this.from);
		}
		
		this.from = from;
		this.allChildren.add(from);
	}
	
	/**
	 * Gets the FROM clause, or null, if none is set.
	 * 
	 * @return The FROM clause.
	 */
	public FromClause getFromClause() {
		return this.from;
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
	
	/**
	 * Sets the GROUP BY clause.
	 * 
	 * @param groupBy The GROUP BY clause.
	 */
	public void setGroupByClause(GroupByClause groupBy)
	{
		if (this.groupBy != null) {
			this.allChildren.remove(this.groupBy);
		}
		
		this.groupBy = groupBy;
		this.allChildren.add(groupBy);
	}
	
	/**
	 * Gets the GROUP BY clause, or null, if none is set.
	 * 
	 * @return The GROUP BY clause.
	 */
	public GroupByClause getGroupByClause() {
		return this.groupBy;
	}
	
	/**
	 * Sets the HAVING clause.
	 * 
	 * @param having The HAVING clause.
	 */
	public void setHavingClause(HavingClause having)
	{
		if (this.having != null) {
			this.allChildren.remove(this.having);
		}
		
		this.having = having;
		this.allChildren.add(having);
	}
	
	/**
	 * Gets the HAVING clause, or null, if none is set.
	 * 
	 * @return The HAVING clause.
	 */
	public HavingClause getHavingClause() {
		return this.having;
	}
	
	/**
	 * Sets the order by clause.
	 * 
	 * @param orderBy The order by clause.
	 */
	public void setOrderByClause(OrderByClause orderBy)
	{
		if (this.orderBy != null) {
			this.allChildren.remove(this.orderBy);
		}
		
		this.orderBy = orderBy;
		this.allChildren.add(orderBy);
	}
	
	/**
	 * Gets the order by clause, or null, if none is set.
	 * 
	 * @return The order by clause.
	 */
	public OrderByClause getOrderByClause() {
		return this.orderBy;
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
	public Iterator<ParseTreeNode> getChildren()
	{
		return this.allChildren.iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder();
		
		if (this.select != null) {
			bld.append(this.select.getNodeContents());
		}
		if (this.from != null) {
			bld.append(' ').append(this.from.getNodeContents());
		}
		if (this.where != null) {
			bld.append(' ').append(this.where.getNodeContents());
		}
		if (this.groupBy != null) {
			bld.append(' ').append(this.groupBy.getNodeContents());
		}
		if (this.having != null) {
			bld.append(' ').append(this.having.getNodeContents());
		}
		if (this.orderBy != null) {
			bld.append(' ').append(this.orderBy.getNodeContents());
		}
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "Query";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return this.allChildren.size();
	}
	
	
	/**
	 * Checks if the combination of clauses in this query is consistent.
	 * SELECT and FROM are required and HAVING implies that GROUP BY is set.
	 *  
	 * @return true, if the query is valid in the form of its clauses.
	 */
	public boolean isValidClauseCombination()
	{
		return (this.select != null && this.from != null && (this.having == null || this.groupBy == null));
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof SelectQuery)
		{
			SelectQuery other = (SelectQuery) node;
			
			return ( (this.select == null && other.select == null) ||
					 (this.select != null && other.select != null &&
					  this.select.isIdenticalTo(other.select))
				   ) && (
					 (this.from == null && other.from == null) ||
					 (this.from != null && other.from != null &&
					  this.from.isIdenticalTo(other.from))
				   ) && (
				     (this.where == null && other.where == null) ||
					 (this.where != null && other.where != null &&
					  this.where.isIdenticalTo(other.where))
				   ) && (
				     (this.groupBy == null && other.groupBy == null) ||
					 (this.groupBy != null && other.groupBy != null &&
					  this.groupBy.isIdenticalTo(other.groupBy))
				   ) && (
				     (this.having == null && other.having == null) ||
					 (this.having != null && other.having != null &&
					  this.having.isIdenticalTo(other.having))
				   ) && (
				     (this.orderBy == null && other.orderBy == null) ||
					 (this.orderBy != null && other.orderBy != null &&
					  this.orderBy.isIdenticalTo(other.orderBy))
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
