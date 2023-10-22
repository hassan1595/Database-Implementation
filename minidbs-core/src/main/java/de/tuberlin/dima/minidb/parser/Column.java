package de.tuberlin.dima.minidb.parser;


import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;


/**
 * A parse tree node that represents a column expressed as <i>&lt;tab-name&gt;.&lt;column-name&gt;.
 * A column is created during parsing through table alias name and column name. It can optionally
 * hold a reference to a TableReference object, to be pointing to the table that the column belongs to.
 * This is only relevant for semantical checking.
 * </i>.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class Column implements ParseTreeNode
{

	/**
	 * The name of the column.
	 */
	protected final String columnName;
	
	/**
	 * The alias name of the table that produces this column.
	 */
	protected final String tableAlias;
	
	/**
	 * The reference to the table (= resolved alias).
	 */
	private TableReference tableRef;
	
	
	/**
	 * Creates a column contained in a table given through the alias.
	 * 
	 * @param name The column name.
	 * @param tableAlias The table alias name.
	 */
	public Column(String name, String tableAlias)
	{
		this.columnName = name;
		this.tableAlias = tableAlias;
	}	
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<ParseTreeNode> getChildren()
	{
		return this.tableRef == null ? 
				Collections.<ParseTreeNode>emptyList().iterator() :
				Arrays.asList(new ParseTreeNode[] {this.tableRef}).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		return this.tableAlias + "." + this.columnName;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "Column";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return this.tableRef == null ? 0 : 1;
	}


	/**
	 * Gets the reference to the table containing the column.
	 * 
	 * @return The containing table.
	 */
	public TableReference getTableRef()
	{
		return this.tableRef;
	}

	/**
	 * Sets the reference to the containing table.
	 * 
	 * @param tableRef The table reference.
	 */
	public void setTableRef(TableReference tableRef)
	{
		this.tableRef = tableRef;
	}

	/**
	 * Gets the name of the column.
	 * 
	 * @return The column name.
	 */
	public String getColumnName()
	{
		return this.columnName;
	}

	/**
	 * Gets the table alias name.
	 * 
	 * @return The table alias name.
	 */
	public String getTableAlias()
	{
		return this.tableAlias;
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
		if (node instanceof Column)
		{
			Column col = (Column) node;
			return ( (this.columnName == null && col.columnName == null) ||
					 (this.columnName != null && col.columnName != null &&
					  this.columnName.equalsIgnoreCase(col.columnName)))
					&&
					( (this.tableRef == null && col.tableRef == null) ||
					  (this.tableRef != null && col.tableRef != null && 
				       this.tableRef.isIdenticalTo(col.tableRef)))
				    &&
				    ( (this.tableAlias == null && col.tableAlias == null) ||
				      (this.tableAlias != null && col.tableAlias != null &&
				       this.tableAlias.equalsIgnoreCase(col.tableAlias)));
		}
		
		return false;
	}

}
