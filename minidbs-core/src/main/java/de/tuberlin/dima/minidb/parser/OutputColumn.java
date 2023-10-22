package de.tuberlin.dima.minidb.parser;


import java.util.Arrays;
import java.util.Iterator;

import de.tuberlin.dima.minidb.parser.Token.TokenType;


/**
 * A parse tree node that represents a result column.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OutputColumn implements ParseTreeNode
{
	
	/**
	 * The types of aggregation that are possible for that output column.
	 */
	public enum AggregationType
	{
		/**
		 * Type for a column that is not aggregated.
		 */
		NONE,
		
		/**
		 * Type for a column that is aggregated through the COUNT function.
		 */
		COUNT,
		
		/**
		 * Type for a column that is aggregated through the SUM function.
		 */
		SUM,
		
		/**
		 * Type for a column that is aggregated through the AVG function.
		 */
		AVG,
		
		/**
		 * Type for a column that is aggregated through the MIN function.
		 */
		MIN,
		
		/**
		 * Type for a column that is aggregated through the MAX function.
		 */
		MAX;

		/**
		 * Returns the aggregation type for a given token or null if none matches.
		 * 
		 * @param t The token to identify the aggregation type.
		 * @return aggregation type or null if none matches
		 */
		public static AggregationType getAggregationType(Token t){

			if (t.getType() == TokenType.AGG_AVG) {
				return OutputColumn.AggregationType.AVG;
			}
			else if (t.getType() == TokenType.AGG_SUM) {
				return OutputColumn.AggregationType.SUM;
			}
			else if (t.getType() == TokenType.AGG_MAX) {
				return OutputColumn.AggregationType.MAX;
			}
			else if (t.getType() == TokenType.AGG_MIN) {
				return OutputColumn.AggregationType.MIN;
			}
			else if (t.getType() == TokenType.AGG_COUNT) {
				return OutputColumn.AggregationType.COUNT;
			}			
			return null;
		}
	}
	
	
	/**
	 * The name of the result column referenced by this node.
	 */
	protected final String name;
	
	/**
	 * The column that is produced.
	 */
	protected final Column column;
	
	/**
	 * The type of aggregation that is performed on this column.
	 */
	protected final AggregationType aggType;
	
	/**
	 * The expression inside the aggregation function.
	 */
	protected final ParseTreeNode expression; 
	
	/**
	 * Create an result column with a given name for the given column.
	 * 
	 * @param col  The column that is produced.
	 * @param name The name of the result column.
	 */
	public OutputColumn(Column col, String name)
	{
		this(col, name, AggregationType.NONE, col);
	}
	
	/**
	 * Create an result column with a given name for the given column.
	 * 
	 * @param col  The column that is produced.
	 * @param name The name of the result column.
	 * @param agg The aggregation applied on this column.
	 * @param expr The expression inside the aggregation
	 */
	public OutputColumn(Column col, String name, AggregationType agg, ParseTreeNode expr)
	{
		this.column = col;
		this.name = name;
		this.aggType = agg;
		this.expression = expr;
	}
	
	
	/**
	 * Gets the name of the result column that is referenced.
	 * 
	 * @return The result column name.
	 */
	public String getResultColumnName()
	{
		return this.name;
	}
	
	/**
	 * Gets the type of aggregation performed by this output column.
	 * 
	 * @return The aggregation type.
	 */
	public AggregationType getAggType()
	{
		return this.aggType;
	}

	/**
	 * Gets the column that is produced here.
	 * 
	 * @return The produced column.
	 */
	public Column getColumn()
	{
		return this.column;
	}

	/**
	 * Gets the expression of this output column.
	 * 
	 * @return The expression of the column.
	 */
	public ParseTreeNode getExpression()
	{
		return this.expression;
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getChildren()
	 */
	@Override
	public Iterator<ParseTreeNode> getChildren()
	{
		return Arrays.asList(new ParseTreeNode[] {this.column}).iterator();
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		if(this.column == null)
		{ 
			return this.name;
		} else 
		{
		
			return this.aggType == AggregationType.NONE ? 
					this.expression.getNodeContents() + " AS " + this.name : 
					this.aggType.name() + "(" + this.expression.getNodeContents() + ") AS " + this.name;
		}
		
//		if (column == null) {
//			return name;
//		}
//		else {
//			return aggType == AggregationType.NONE ?
//					column.getTableAlias() + "." + column.getColumnName() + " AS " + name :
//					aggType.name() + "(" + column.tableAlias + "." +
//					column.getColumnName() + ") AS " + name;
//		}
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "Output Column";
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNumberOfChildren()
	 */
	@Override
	public int getNumberOfChildren()
	{
		return 1;
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
		if (node != null && node instanceof OutputColumn)
		{
			OutputColumn other = (OutputColumn) node;
			
			return ( (this.name == null && other.name == null) ||
					 (this.name != null && other.name != null &&
					  this.name.equalsIgnoreCase(other.name))
				   ) && (
					 (this.column == null && other.column == null) ||
					 (this.column != null && other.column != null &&
					  this.column.isIdenticalTo(other.column))
				   ) && (this.aggType == other.aggType)
				   && ( (this.expression == null && other.expression == null) ||
						(this.expression != null && other.expression != null &&
						 this.expression.isIdenticalTo(other.expression))
				   );
		}
		
		return false;
		
	}

}
