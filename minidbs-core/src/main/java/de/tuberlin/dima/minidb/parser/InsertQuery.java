package de.tuberlin.dima.minidb.parser;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Class representing a parsed INSERT query. Insert queries can be of the type:
 * 
 * INSERT INTO table VALUES (...), (...), (...)
 * or
 * INSERT INTO table SELECT ...
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class InsertQuery extends ParsedQuery
{
	
	/**
	 * The name of the table to insert into.
	 */
	protected String tableName;
	
	/**
	 * The values clauses containing the values to insert.
	 * This field is exclusive with the field <code>select</code>.
	 */
	protected List<ValuesClause> valuesClauses;
	
	
	/**
	 * The select clause in the query. This field is exclusive with the field
	 * <code>valuesClauses</code>.
	 */
	protected SelectQuery select;
	
	
	/**
	 * Creates a plain empty insert query.
	 */
	public InsertQuery()
	{
		this.valuesClauses = new ArrayList<ValuesClause>();
	}
	
	
	/**
	 * Add a values clause to this query.
	 * 
	 * @param values The values clause to add.
	 */
	public void addValuesClause(ValuesClause values)
	{
		this.valuesClauses.add(values);
		this.select = null;
	}

	/**
	 * Gets the values clause at the given position.
	 * 
	 * @param index The position for the values clause to get.
	 * @return The retrieved values clause.
	 */
	public ValuesClause getValuesClause(int index)
	{
		return this.valuesClauses.get(index);
	}

	/**
	 * Sets the values clause at the given position to the given clause.
	 * 
	 * @param values The new clause to set.
	 * @param index The position set set the new clause to.
	 */
	public void setValuesClause(ValuesClause values, int index)
	{
		this.valuesClauses.set(index, values);
	}
	
	/**
	 * Gets the number of values clauses in this query.
	 * 
	 * @return The number of values clauses.
	 */
	public int getNumberOfValuesClauses()
	{
		return this.valuesClauses.size();
	}
	
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<? extends ParseTreeNode> getChildren()
	{
		if (this.select != null) {
			return Arrays.asList(new SelectQuery[] {this.select}).iterator();
		}
		else {
			return this.valuesClauses.iterator();
		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		StringBuilder bld = new StringBuilder("INSERT INTO ");
		bld.append(this.tableName).append(' ');
		
		if (this.select == null) {
			bld.append("VALUES ");
			for (int i = 0; i < this.valuesClauses.size(); i++) {
				bld.append(this.valuesClauses.get(i));
				if (i < this.valuesClauses.size() - 1) {
					bld.append(", ");
				}
			}
		}
		else {
			bld.append(this.select.getNodeContents());
		}
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "INSERT INTO";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return this.valuesClauses.size() + (this.select == null ? 0 : 1);
	}



	/**
	 * Gets the name of the table in this insert query.
	 * 
	 * @return The table name.
	 */
	public String getTableName()
	{
		return this.tableName;
	}


	/**
	 * Sets the name of the table in this insert query.
	 * 
	 * @param tableName The table name.
	 */
	public void setTableName(String tableName)
	{
		this.tableName = tableName;
	}

	/**
	 * Gets the select clause in this insert query.
	 * 
	 * @return The select clause.
	 */
	public SelectQuery getSelectQuery()
	{
		return this.select;
	}

	/**
	 * Sets the select clause in this insert query.
	 * 
	 * @param select The select clause.
	 */
	public void setSelectQuery(SelectQuery select)
	{
		if (select != null) {
			this.select = select;
			this.valuesClauses.clear();
		}
	}


	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		if (node != null && node instanceof InsertQuery) {
			InsertQuery other = (InsertQuery) node;
		
			
			// check depending on values or select clause
			if (this.valuesClauses.size() == other.valuesClauses.size())
			{
				outer: for (int i = 0; i < this.valuesClauses.size(); i++) {
					ValuesClause e = this.valuesClauses.get(i);
					// check if this outer element is contained in the inner
					for (int k = 0; k < other.valuesClauses.size(); k++) {
						ValuesClause v = other.valuesClauses.get(i);
						if (e.isIdenticalTo(v)) {
							continue outer;
						}
					}
					// we get here, if we did not find the element in the inner
					return false;
				}
				
				// check table name and select clause
				return ( (this.tableName == null && other.tableName == null) ||
				         (this.tableName != null && other.tableName != null &&
				          this.tableName.equalsIgnoreCase(other.tableName))
				       ) && (
				         (this.select == null && other.select == null) ||
				         (this.select != null && other.select != null &&
				          this.select.isIdenticalTo(other.select)));
			}
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
