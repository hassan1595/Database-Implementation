package de.tuberlin.dima.minidb.catalogue;


/**
 * A simple bean that describes the statistics of an index. A plain bean
 * assumes the default values for the index which are:
 * <ul>
 *   <li>Tree-Depth: 3</li>
 *   <li>Number of Leaf Pages: 500 pages</li>
 * </ul>
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class IndexStatistics
{
	/**
	 * The number of levels that the B-Tree has (including root and leaves).
	 */
	private int treeDepth;
	
	/**
	 * The number of leaf pages in the B-Tree.
	 */
	private int numberOfLeaves;
	
	
	
	/**
	 * Creates a new IndexStatistics bean with the default values.
	 */
	public IndexStatistics()
	{
		this.treeDepth = 3;
		this.numberOfLeaves = 500;
	}



	/**
     * Gets the treeDepth from this IndexStatistics.
     *
     * @return The treeDepth.
     */
    public int getTreeDepth()
    {
    	return this.treeDepth;
    }

	/**
     * Gets the numberOfLeaves from this IndexStatistics.
     *
     * @return The numberOfLeaves.
     */
    public int getNumberOfLeaves()
    {
    	return this.numberOfLeaves;
    }

	/**
     * Sets the treeDepth for this IndexStatistics.
     *
     * @param treeDepth The treeDepth to set.
     */
    public void setTreeDepth(int treeDepth)
    {
    	this.treeDepth = treeDepth;
    }

	/**
     * Sets the numberOfLeaves for this IndexStatistics.
     *
     * @param numberOfLeaves The numberOfLeaves to set.
     */
    public void setNumberOfLeaves(int numberOfLeaves)
    {
    	this.numberOfLeaves = numberOfLeaves;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
	public String toString()
    {
    	return "Tree levels: " + this.treeDepth + ", Number of Leaves: " + this.numberOfLeaves;
    }
}
