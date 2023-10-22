package de.tuberlin.dima.minidb.semantics;


import de.tuberlin.dima.minidb.core.DataType;


/**
 * Simple description of a column. Contains the Data type of the column, as
 * well as its index in the relation it comes form. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class Column
{
	/**
	 * The constant used as the column index for RID columns.
	 */
	public static final int RID_COLUMN_INDEX = -1;
	
	/**
	 * The relation the column originates from.
	 */
	private final Relation origin;
	
	/**
	 * The column's data type.
	 */
	private final DataType dataType;
	
	/**
	 * The index of the column in its original table or nested query.
	 */
	private final int columnIndex;

	

	/**
	 * Creates a new column, originating from the given relation.
	 * The column may come from a base table, or a nested query.
	 * 
	 * @param origin The relation that the column comes from
	 * @param type The data type of the column.
	 * @param columnIndex The index of the column in its original relation.
	 */
	public Column(Relation origin, DataType type, int columnIndex)
	{
		this.origin = origin;
		this.dataType = type;
		this.columnIndex = columnIndex;
	}

	/**
	 * Gets the relation that produces this column.
	 * 
	 * @return The relation producing the column.
	 */
	public Relation getRelation()
	{
		return this.origin;
	}
	
	/**
	 * Gets the columnIndex from this column. If the column originates from a base table, this is the
	 * index of the column in the base table. If the column is produced by a sub-query, then this is 
	 * the index of the column in the result set.
	 *
	 * @return The columnIndex.
	 */
	public int getColumnIndex()
	{
		return this.columnIndex;
	}
	
	/**
	 * Gets the data type of this column.
	 * 
	 * @return The column's data type.
	 */
	public DataType getDataType()
	{
		return this.dataType;
	}
	
	/**
	 * Checks whether this column is an RID column.
	 * 
	 * @return True, if this is an RID column, false otherwise.
	 */
	public boolean isRID()
	{
		return this.columnIndex == RID_COLUMN_INDEX;
	}

	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder();
		
		bld.append(this.origin.toString());
		bld.append('.').append(this.columnIndex).append(' ').append('(');
		bld.append(this.dataType);
		bld.append(')');
		
		return bld.toString();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.columnIndex;
		result = prime * result + ((this.origin == null) ? 0 : this.origin.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		
		if (obj != null && obj instanceof Column) {
			Column other = (Column) obj;
			return (this.columnIndex == other.columnIndex && this.origin == other.origin);
		}
		else {
			return false;
		}
	}
}
