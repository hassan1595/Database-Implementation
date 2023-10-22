package de.tuberlin.dima.minidb.core;


/**
 * This interface has to be implemented by data types that can be part of basic 
 * arithmetic calculations. Such calculations occur for example in aggregation
 * functions like SUM or AVG.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ArithmeticType<T extends DataField> {
	
	/**
	 * Adds the value of another field to the value of this field.
	 * 
	 * @param other The fields whose value is to add.
	 */
	public void add(T other);
	
	/**
	 * Subtracts the value of another field from the value of this field.
	 * 
	 * @param other The fields whose value is to be subtracted.
	 */
	public void subtract(T other);
	
	/**
	 * Multiplies the value of this field with the value of another field.
	 * 
	 * @param other The fields whose value this field multiplies its value with.
	 */
	public void multiplyWith(T other);
	
	/**
	 * Divides the value of this field by the value of another field.
	 * 
	 * @param other The fields by whose value this field's value is divided.
	 */
	public void divideBy(T other);
	
	/**
	 * Adds a value to the value of this field.
	 * 
	 * @param summand The value to add.
	 */
	public void add(int summand);
	
	/**
	 * Subtracts a value from the value of this field.
	 * 
	 * @param minuend The value to be subtracted.
	 */
	public void subtract(int minuend);
	
	/**
	 * Multiplies the value of this field with the given value.
	 * 
	 * @param factor The value to multiply with.
	 */
	public void multiplyWith(int factor);
	
	/**
	 * Divides  the value of this field by the given value.
	 * 
	 * @param divisor The value to divide by.
	 */
	public void divideBy(int divisor);
	
	/**
	 * Checks if this field holds a zero.
	 * 
	 * @return True, if this field is zero, false otherwise.
	 */
	public boolean isZero();
	
	/**
	 * Gets the zero as the data type behind this arithmetic type.
	 * 
	 * @return Zero as the data type behind this arithmetic type.
	 */
	public ArithmeticType<T> createZero();
	
	/**
	 * Gets this fields value casted to a long.
	 * 
	 * @return A long for the value of this field.
	 */
	public long asLong();
	
	/**
	 * Gets this fields value casted to a double.
	 * 
	 * @return A double for the value of this field.
	 */
	public double asDouble();
}
