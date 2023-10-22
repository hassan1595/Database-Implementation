package de.tuberlin.dima.minidb.parser;


/**
 * Class representing an integer literal.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IntegerLiteral extends Literal
{

	/**
	 * The integer number in this literal.
	 */
	protected long number;

	
	/**
	 * Creates an integer literal for the given long.
	 * 
	 * @param num The number for the literal.
	 */
	public IntegerLiteral(long num)
	{
		this.number = num; 
	}
	
	/**
	 * Gets the number in this literal.
	 * 
	 * @return The number.
	 */
	public long getNumber()
	{
		return this.number;
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeContents()
	 */
	@Override
	public String getNodeContents()
	{
		return String.valueOf(this.number);
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#getNodeName()
	 */
	@Override
	public String getNodeName()
	{
		return "Integer Literal";
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		return (node != null) && (node instanceof IntegerLiteral) &&
		       ( ((IntegerLiteral) node).number == this.number );
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.Literal#getLiteralValue()
	 */
	@Override
	public String getLiteralValue()
	{
		return String.valueOf(this.number);
	}
}
