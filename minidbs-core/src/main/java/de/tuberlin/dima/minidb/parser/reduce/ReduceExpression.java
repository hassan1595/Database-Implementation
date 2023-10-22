package de.tuberlin.dima.minidb.parser.reduce;

/**
 * Interface yielding the functionality to reduce column expressions for different operators.
 * 
 * @author Michael Saecker
 */
public interface ReduceExpression 
{
	/**
	 * Reduces two long values by evaluating the expression.
	 * 
	 * @param left The left value of the operand.
	 * @param right The right value of the operand.
	 * @return The reduced (evaluated) expression
	 */
	public long reduce(long left, long right);

	/**
	 * Reduces a long and a double value by evaluating the expression.
	 * 
	 * @param left The left value of the operand.
	 * @param right The right value of the operand.
	 * @return The reduced (evaluated) expression
	 */
	public double reduce(long left, double right);

	/**
	 * Reduces a double and long value by evaluating the expression.
	 * 
	 * @param left The left value of the operand.
	 * @param right The right value of the operand.
	 * @return The reduced (evaluated) expression
	 */
	public double reduce(double left, long right);

	/**
	 * Reduces two double values by evaluating the expression.
	 * 
	 * @param left The left value of the operand.
	 * @param right The right value of the operand.
	 * @return The reduced (evaluated) expression
	 */
	public double reduce(double left, double right);		
}
