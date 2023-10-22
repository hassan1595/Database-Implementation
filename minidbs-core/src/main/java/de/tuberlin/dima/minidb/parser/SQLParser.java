package de.tuberlin.dima.minidb.parser;


/**
 * Specification of the interface for a parser that accepts a SQL string
 * and returns a syntax tree representation of the string. References, such as
 * from columns to their containing tables, are resolved.
 *   
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface SQLParser
{

	/**
	 * Root method for invoking the parser to parse a SQL query.
	 * 
	 * @return The parsed SQL query.
	 * @throws ParseException Thrown, when the statement was syntactically wrong.
	 */
	public ParsedQuery parse() throws ParseException;
	
}
