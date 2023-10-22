package de.tuberlin.dima.minidb.parser;


/**
 * Class representing a real literal.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class RealLiteral extends Literal
{

	/**
	 * The real number in this literal.
	 */
	protected double number;

	
	/**
	 * Creates an real literal for the given long.
	 * 
	 * @param num The number for the literal.
	 */
	public RealLiteral(double num)
	{
		this.number = num;
	}
	
	/**
	 * Gets the number in this literal.
	 * 
	 * @return The number.
	 */
	public double getNumber()
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
		return "Real Literal";
	}
	
	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.parser.ParseTreeNode#isIdenticalTo(de.tuberlin.dima.minidb.parser.ParseTreeNode)
	 */
	@Override
	public boolean isIdenticalTo(ParseTreeNode node)
	{
		return (node != null) && (node instanceof RealLiteral) &&
		       ( ((RealLiteral) node).number == this.number );
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
