package de.tuberlin.dima.minidb.parser;


/**
 * Class representing a string literal.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class StringLiteral extends Literal
{

	/**
	 * The string in this literal.
	 */
	protected String theString;

	
	/**
	 * Creates a string literal for the given string.
	 * 
	 * @param string The string for the literal.
	 */
	public StringLiteral(String string)
	{
		this.theString = string; 
	}
	
	/**
	 * Gets the string in this literal.
	 * 
	 * @return The string.
	 */
	public String getString()
	{
		return this.theString;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		return '"' + this.theString + '"';
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "String Literal";
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		return (node != null) && (node instanceof StringLiteral) &&
		       ( ((StringLiteral) node).theString.equals(this.theString));
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.Literal#getLiteralValue()
	 */
	@Override
	public String getLiteralValue()
	{
		return this.theString;
	}



}
