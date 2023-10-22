package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Iterator;


/**
 * A parse tree node that represents a referenced table or 
 * a referenced select query in the FROM clause.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 * @author Michael Saecker (extended for nested queries)
 */
public class TableReference implements ParseTreeNode
{
	/**
	 * The name of the table referenced by this node.
	 */
	protected final String tableName;
	
	/**
	 * The select query referenced by this node.
	 */
	protected final SelectQuery selectQuery;
	
	/**
	 * The alias name of the table 
	 */
	protected final String aliasName;
	
	
	/**
	 * Create a reference node for the table with the given name.
	 * 
	 * @param tabName The name of the table.
	 * @param alias The alias of the table.
	 */
	public TableReference(String tabName, String alias)
	{
		this.tableName = tabName;
		this.aliasName = alias;
		this.selectQuery = null;
	}
	
	/**
	 * Create a reference node for the select query with the given alias.
	 * 
	 * @param select The select query referenced.
	 * @param alias The alias of the select query.
	 */
	public TableReference(SelectQuery select, String alias)
	{
		this.tableName = null;
		this.aliasName = alias;
		this.selectQuery = select;
	}
	
	/**
	 * Gets the name of the table that is referenced.
	 * 
	 * @return The table name.
	 */
	public String getTableName()
	{
		return this.tableName;
	}
	
	/**
	 * Gets the select query that is referenced.
	 * 
	 * @return The select query.
	 */
	public SelectQuery getSelectQuery()
	{
		return this.selectQuery;
	}
	
	/**
	 * Gets the alias name of the table that is referenced.
	 * 
	 * @return The table's alias name.
	 */
	public String getAliasName()
	{
		return this.aliasName;
	}

	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<ParseTreeNode> getChildren()
	{
		if(this.selectQuery != null)
		{
			ArrayList<ParseTreeNode> arr = new ArrayList<ParseTreeNode>(1);
			arr.add(this.selectQuery);
			return arr.iterator();
		} else 
		{
			return new ArrayList<ParseTreeNode>(0).iterator();
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		if(this.selectQuery != null)
		{
			return '(' + this.selectQuery.getNodeContents() + ')' + ' ' + this.aliasName;
		} else 
		{
			return this.tableName + ' ' + this.aliasName;
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "Table Reference";
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		if(this.selectQuery != null)
		{
			return 1;
		} else 
		{
			return 0;
		}
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
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof TableReference)
		{
			TableReference other = (TableReference) node;
			
			return ( (
					this.tableName == null && other.tableName == null) ||
					 (this.tableName != null && other.tableName != null &&
					  this.tableName.equalsIgnoreCase(other.tableName))
				   ) && (
					 (this.aliasName == null && other.aliasName == null) ||
					 (this.aliasName != null && other.aliasName != null &&
					  this.aliasName.equalsIgnoreCase(other.aliasName))
				   ) && (
					 (this.selectQuery == null && other.selectQuery == null) ||
					 (this.selectQuery != null && other.selectQuery != null &&
					 this.selectQuery.isIdenticalTo(other.selectQuery))
				   );
		}
		return false;
	}
}
