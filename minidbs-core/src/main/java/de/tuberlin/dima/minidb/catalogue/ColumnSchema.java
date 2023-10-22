package de.tuberlin.dima.minidb.catalogue;


import de.tuberlin.dima.minidb.core.DataType;


/**
 * The description of the schema for a column. Contains the name, the data type, and
 * flags about uniqueness and null-ability.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ColumnSchema
{
	/**
	 * The name of the column.
	 */
	private String columnName;
	
	/**
	 * The data type of the fields in this column.
	 */
	private DataType dataType;
	
	/**
	 * A flag indicating that the column allows NULL values.
	 */
	private boolean nullable;
	
	/**
	 * A flag indicating that the column has unique entries.
	 */
	private boolean unique;

	
	
	/**
	 * Create a new instance of the column schema. The column allows by default
	 * NULL values and has duplicates.
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 */
	protected ColumnSchema(String name, DataType type)
	{
		this.columnName = name;
		this.dataType = type;
		this.nullable = true;
		this.unique = false;
	}
	
	/**
	 * Create a new instance of the column schema. The column allows by default duplicates
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 * @param nullable Whether the column is allowed to accept NULL values.
	 */
	protected ColumnSchema(String name, DataType type, boolean nullable)
	{
		this.columnName = name;
		this.dataType = type;
		this.nullable = nullable;
		this.unique = false;
	}

	/**
	 * Create a new instance of the column schema.
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 * @param nullable Whether the column is allowed to accept NULL values.
	 * @param unique Whether the column has unique values.
	 */
	protected ColumnSchema(String name, DataType type, boolean nullable, boolean unique)
	{
		this.columnName = name;
		this.dataType = type;
		this.nullable = nullable;
		this.unique = unique;
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
	 * Gets the data type for this column.
	 * 
	 * @return The column data type.
	 */
	public DataType getDataType()
    {
    	return this.dataType;
    }

	/**
	 * Checks, if the column allows NULL values.
	 * 
	 * @return true, if the column allows NULL values, false otherwise.
	 */
	public boolean isNullable()
    {
    	return this.nullable;
    }
	
	/**
	 * Checks, if the column has unique entries.
	 * 
	 * @return true, if the column has unique entries.
	 */
	public boolean isUnique()
	{
		return this.unique;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder(this.columnName);
		bld.append(": ").append(this.dataType);
		
		if (this.unique) {
			bld.append(" UNIQUE");
		}
		
		if (!this.nullable) {
			bld.append(" NOT NULL");
		}
		
		return bld.toString();
	}
	
	// ------------------------------------------------------------------------
	
	/**
	 * Create a new instance of the column schema. The column allows by default
	 * NULL values and has duplicates.
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 */
	public static  ColumnSchema createColumnSchema(
			String name, DataType type)
	{
		return new ColumnSchema(name, type);
	}
	
	/**
	 * Create a new instance of the column schema. The column allows by default duplicates
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 * @param nullable Whether the column is allowed to accept NULL values.
	 */
	public static ColumnSchema createColumnSchema(
			String name, DataType type, boolean nullable)
	{
		return new ColumnSchema(name, type, nullable);
	}

	/**
	 * Create a new instance of the column schema.
	 * 
	 * @param name The name of the column.
	 * @param type The data type for the fields in this column.
	 * @param nullable Whether the column is allowed to accept NULL values.
	 * @param unique Whether the column has unique values.
	 */
	public static ColumnSchema createColumnSchema(
			String name, DataType type, boolean nullable, boolean unique)
	{
		return new ColumnSchema(name, type, nullable, unique);
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ColumnSchema)) return false;
		ColumnSchema other = (ColumnSchema) o;
		// Ensure that both column name and type are identical.
		boolean equals = columnName.equals(other.columnName);
		equals &= (dataType.getBasicType().ordinal() == other.dataType.getBasicType().ordinal());
		return equals;		
	}
}
