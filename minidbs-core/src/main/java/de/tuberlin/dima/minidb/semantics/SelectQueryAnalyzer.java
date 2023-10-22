package de.tuberlin.dima.minidb.semantics;


import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.parser.SelectQuery;


/**
 * The interface for classes that implement a semantical analyzer for select queries. 
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface SelectQueryAnalyzer
{
	
	/**
	 * Analyzes the given select query and returns an internal representation.
	 * 
	 * @param query The parse-tree of the query that needs to be analyzed.
	 * @param catalogue The catalogue which contains and describes all tables that are accessible by the query.
	 * @return The analyzed and rewritten query.
	 * @throws QuerySemanticsInvalidException Thrown whenever the analyzer finds the query to contain invalid
	 *                                        semantics, such as referencing a non-existing table or column.
	 */
	public AnalyzedSelectQuery analyzeQuery(SelectQuery query, Catalogue catalogue)
	throws QuerySemanticsInvalidException;
}
