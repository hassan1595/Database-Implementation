package de.tuberlin.dima.minidb.catalogue;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.cache.UnsupportedPageSizeException;
import de.tuberlin.dima.minidb.util.Pair;


/**
 * A simple description of the schema of a table. This is a catalogue stub.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TableSchema
{	
	/**
	 * The number of bytes per page for this table. 
	 */
	private final PageSize pageSize;
	
	/**
	 * The list of columns contained in this table. 
	 */
	private final List<ColumnSchema> columns;
	
	/**
	 * The map from column names to column-position pairs.
	 */
	private final Map<String, Pair<ColumnSchema, Integer>> columnsByName;

	
	/**
	 * Creates a new table schema for the default page size of 4096 bytes.
	 */
	public TableSchema()
	{
		this(PageSize.getDefaultPageSize());
	}
	
	/**
	 * Creates a new table schema for the given page size. 
	 * 
	 * @param pageSize The number of bytes per page for this table.
	 * @throws IllegalArgumentException Thrown, if the page size is invalid. 
	 */
	public TableSchema(int pageSize)
	{
		try {
			this.pageSize = PageSize.getPageSize(pageSize);
		}
		catch (UnsupportedPageSizeException uspex) {
			throw new IllegalArgumentException("The given page size is not supported.", uspex);
		}
		
		this.columns = new ArrayList<ColumnSchema>();
		this.columnsByName = new HashMap<String, Pair<ColumnSchema,Integer>>();
	}
	
	/**
	 * Creates a new table schema for the given page size. 
	 * 
	 * @param pageSize The number of bytes per page for this table.
	 * @throws IllegalArgumentException Thrown, if the page size is invalid. 
	 */
	public TableSchema(PageSize pageSize)
	{
		this.pageSize = pageSize;
		this.columns = new ArrayList<ColumnSchema>();
		this.columnsByName = new HashMap<String, Pair<ColumnSchema,Integer>>();
	}
	
	
	/**
	 * Gets the size of this tables pages.
	 * 
	 * @return The table's page size.
	 */
	public PageSize getPageSize()
	{
		return this.pageSize;
	}
	
	/**
	 * Gets the number of columns in this table schema.
	 * 
	 * @return The number of column in this table schema.
	 */
	public int getNumberOfColumns()
	{
		return this.columns.size();
	}
	
	/**
	 * Gets the schema for the column with the given index.
	 * 
	 * @param index The index of the column whose schema is to get.
	 * @return The fetched column schema.
	 */
	public ColumnSchema getColumn(int index)
	{
		return this.columns.get(index);
	}
	
	/**
	 * Gets the schema for the column with the given name.
	 * 
	 * @param name The name of the column to get.
	 * @return The fetched column schema, together with the column's index in the table.
	 */
	public Pair<ColumnSchema, Integer> getColumn(String name)
	{
		return this.columnsByName.get(name.toLowerCase(Constants.CASE_LOCALE));
	}
	
	/**
	 * Adds a column schema to this table schema. The column schema will be appended
	 * to the end.
	 * 
	 * @param col The column schema to append.
	 */
	public void addColumn(ColumnSchema col)
	{
		int index = this.columns.size();
		
		// add the column to the list
		this.columns.add(col);
		
		// add the column to the by-name map
		Pair<ColumnSchema, Integer> pair = new Pair<ColumnSchema, Integer>(col, index);
		this.columnsByName.put(col.getColumnName().toLowerCase(Constants.CASE_LOCALE), pair);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		StringBuilder bld = new StringBuilder("(");
		for (int i = 0; i < this.columns.size(); i++) {
			bld.append(this.columns.get(i).toString()).append(", ");
		}
		
		if (bld.length() > 1) {
			bld.setLength(bld.length() - 2);
		}
		
		bld.append(") PAGE_SIZE ").append(this.pageSize.getNumberOfBytes());
		
		return bld.toString();
	}
}
