package de.tuberlin.dima.minidb.util;


/**
 * Pair container to be able to return multiple values.
 * 
 * @author Michael Saecker
 */
public class Pair<A extends Object, B extends Object> {
	
	/**
	 * First value of the tuple.
	 */
	private A first;
	
	/**
	 * Second value of the tuple.
	 */
	private B second;

	/**
	 * Create an empty tuple.
	 */
	public Pair()
	{
		this.first = null;
		this.second = null;
	}
	
	/**
	 * Create a tuple with the given parameters.
	 * 
	 * @param first The first value of tuple.
	 * @param second The second value of tuple.
	 */
	public Pair(A first, B second)
	{
		this.first = first;
		this.second = second;
	}
	
	/**
	 * Returns the first value of the tuple.
	 * 
	 * @return First value of the tuple
	 */
	public A getFirst()
	{
		return this.first;
	}
	
	/**
	 * Returns the second value of the tuple.
	 * 
	 * @return Second value of the tuple
	 */
	public B getSecond()
	{
		return this.second;
	}
	
	/**
	 * Set the first value of the tuple.
	 * 
	 * @param first First value of the tuple.
	 */
	public void setFirst(A first)
	{
		this.first = first;
	}

	/**
	 * Set the second value of the tuple.
	 * 
	 * @param second Second value of the tuple.
	 */
	public void setSecond(B second)
	{
		this.second = second;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object o)
	{
		return (o instanceof Pair) ? ((Pair) o).first.equals(this.first) && ((Pair) o).second.equals(this.second) : false;
	}
	
}

