package de.tuberlin.dima.minidb.standalone;


import java.io.PrintStream;

import de.tuberlin.dima.minidb.ResultHandler;
import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.semantics.ProducedColumn;


/**
 * A result set printing the tuples in a textual representation to a print stream.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PrintStreamResultSet implements ResultHandler
{
	/**
	 * The stream to print the result tuples to.
	 */
	private PrintStream outputStream;
	
	/**
	 * The string describing the print format of the output.
	 */
	private String formatString;
	
	/**
	 * The argument array to the printing calls.
	 */
	private Object[] arrayBuffer;
	
	/**
	 * The counter for the number of returned tuples.
	 */
	private int tupleCounter;
	
	/**
	 * Flag to remember whether an exception had occurred.
	 */
	private boolean exceptionEncountered;
	
	
	
	/**
	 * Creates a new PrintStream result set printing the result tuples
	 * to the given print stream.
	 * 
	 * @param stream The stream to print the result tuples to.
	 */
	public PrintStreamResultSet(PrintStream stream)
	{
		this.outputStream = stream;
	}
	
	
	/* (non-Javadoc)
     * @see de.tuberlin.dima.minidb.ResultSet#setResultSchema(de.tuberlin.dima.minidb.catalogue.ColumnSchema[])
     */
    @Override
	public void openResultSet(ProducedColumn[] columns)
    {
    	this.arrayBuffer = new Object[columns.length];
    	StringBuilder format = new StringBuilder();
    	int totalwidth = 0;
    	
    	// add the format for every column
    	for (int i = 0; i < columns.length; i++) {
    		DataType type = columns[i].getOutputDataType();
    		
    		// determine the column width
    		int width = getBaseTypeLength(type.getBasicType()) * 
    			type.getLength();
    		totalwidth += width + 1;
    		
    		// all columns are basically strings
    		format.append('%');
    		format.append(i + 1);
    		format.append('$');
    		format.append(width);
    		format.append('s');
    		format.append(' ');
    		this.arrayBuffer[i] = columns[i].getColumnAliasName();
    	}
    	
    	// format string is ready
    	format.append('\n');
    	this.formatString = format.toString();
    	
    	// print the header
    	this.outputStream.printf(this.formatString, this.arrayBuffer);
    	
    	// print the separator
    	for (int i = 0; i < totalwidth; i++) {
    		this.outputStream.print('-');
    	}
    	this.outputStream.println();

    	// reset the counter
    	this.tupleCounter = 0;
    	this.exceptionEncountered = false;
    }
    
	/* (non-Javadoc)
     * @see de.tuberlin.dima.minidb.ResultSet#addResultTuple(de.tuberlin.dima.minidb.core.DataTuple)
     */
    @Override
	public void addResultTuple(DataTuple tuple)
    {
		if (this.formatString == null) {
			throw new IllegalStateException("ResultSet is not open.");
		}
		
		// encode every column field as a string
		for (int i = 0; i < tuple.getNumberOfFields(); i++) {
			this.arrayBuffer[i] = tuple.getField(i).encodeAsString().trim();
		}
		
		// print the tuple to the output stream
		this.outputStream.printf(this.formatString, this.arrayBuffer);
		
		this.tupleCounter++;
    }

	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.ResultSet#closeResultSet()
	 */
	@Override
	public void closeResultSet()
	{
		if (!this.exceptionEncountered) {
			this.outputStream.println();
			this.outputStream.print(this.tupleCounter);
			this.outputStream.println(" rows returned.");
			this.outputStream.println();
		}
		
		this.tupleCounter = 0;
		this.arrayBuffer = null;
		this.formatString = null;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.ResultSet#handleException(java.lang.Throwable)
	 */
	@Override
	public void handleException(Throwable t)
	{
		this.exceptionEncountered = true;
		this.outputStream.println();
		this.outputStream.println(t.getMessage());
	}

	/**
	 * Gets the number of characters reserved for a column of the given data type.
	 * 
	 * @param type The type to get the number of characters to reserve for.
	 * @return The number of characters to reserve.
	 */
	private static final int getBaseTypeLength(BasicType type)
	{
		switch (type) {
        case SMALL_INT:
	        return 6;
        case INT:
	        return 11;
        case BIG_INT:
	        return 21;
        case FLOAT:
	        return 21;
        case DOUBLE:
	        return 31;
        case DATE:
	        return 10;
        case TIME:
	        return 23;
        case TIMESTAMP:
	        return 23;
        case CHAR:
	        return 1;
        case VAR_CHAR:
	        return 1;
        default:
	        return 10;
        }
	}
}
