package de.tuberlin.dima.minidb.parser.reduce;

/**
 * Class yielding the functionality to reduce column expressions using multiplication.
 * 
 * @author Michael Saecker
 */
public class MultiplicationReduceExpression implements ReduceExpression {

	@Override
	public long reduce(long left, long right) 
	{		
		return	left * right;
	}

	@Override
	public double reduce(long left, double right) 
	{
		return left * right;
	}

	@Override
	public double reduce(double left, long right) 
	{
		return left * right;
	}

	@Override
	public double reduce(double left, double right) 
	{
		return left * right;
	}

}
