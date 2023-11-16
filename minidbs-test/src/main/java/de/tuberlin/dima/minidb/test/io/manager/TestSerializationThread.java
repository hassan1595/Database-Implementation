package de.tuberlin.dima.minidb.test.io.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.manager.BufferPoolException;

/**
 * A thread testing the serialization by arbitrarily addressing 
 * pages of a certain resource and changing a value in the byte buffer.
 * 
 * @author Michael Saecker
 */
public class TestSerializationThread extends Thread 
{
	/**
	 * Flag deciding whether this thread should continue working.
	 */
	private boolean alive;
	
	/**
	 * The object owning this thread. 
	 */
	private AbstractTestBufferPoolManager parent;
	
	/**
	 * A random number generator.
	 */
	private Random random;
	
	/**
	 * The resource to address.
	 */
	private int resourceId;
	
	/**
	 * A list of page numbers for the resource. 
	 */
	private List<Integer> pageNumbers;
	
	/**
	 * A mapping of page numbers to the number of modifications.
	 */
	private HashMap<Integer, Integer> modifiedValues;
	
	/**
	 * The position where the modifications will be stored.
	 */
	public static int fieldPos = 1200;
	
	/**
	 * A list of keys that will be addressed by this thread.
	 */
	private ArrayList<Integer> modifiedKeys;

	/**
	 * The total number of manipulations this thread should perform.
	 */
	private static int totalManipulations = 1000;

	/**
	 * The current number of modifications performed.
	 */
	private int manipulations;
	
	/**
	 * Constructor initializing this thread.
	 * 
	 * @param parent The owner of this thread.
	 * @param resourceId The resource to address.
	 * @param pageNumbers The page numbers contained in the resource.
	 */
	public TestSerializationThread(AbstractTestBufferPoolManager parent, int resourceId, List<Integer> pageNumbers) 
	{	
		this.alive = true;	
		this.parent = parent;
		this.random = new Random();
		this.resourceId = resourceId;
		this.pageNumbers = pageNumbers;
		this.modifiedValues = new HashMap<Integer, Integer>();
		this.modifiedKeys = new ArrayList<Integer>(100);
		this.manipulations = 0;
	}
	
	/**
	 * Main method executing the serialization test.
	 */
	@Override
	public void run()
	{
		// pick 100 pages from the page numbers and modify them constantly
		for(int i = 0; i < 100; i++)
		{
			Integer pageNumber = this.pageNumbers.get(this.random.nextInt(this.pageNumbers.size()));
			if(this.modifiedValues.get(pageNumber) == null)
			{
				this.modifiedValues.put(pageNumber, 0);
				this.modifiedKeys.add(i, pageNumber);
			}
			else
			{
				i--;
			}
		}
		
		// first wait a random amount of time
		try {
			Thread.sleep(100 + this.random.nextInt(200));
		} catch (InterruptedException e1) {
			// do nothing
		}
		// do
		while(this.alive)
		{
			try 
			{
				try 
				{
					// increase a stored value in the binary buffer
					int pageNumber = this.modifiedKeys.get(this.random.nextInt(100));
					CacheableData cd = this.parent.underTest.getPageAndPin(this.resourceId, pageNumber);
					manipulateBinaryPage(cd.getBuffer(), TestSerializationThread.fieldPos);
					int mods = this.modifiedValues.get(pageNumber);
					mods++;
					this.modifiedValues.put(pageNumber, mods);
				} 
				catch (BufferPoolException e) 
				{
					e.printStackTrace();
					System.out.println(this.manipulations);
				} 
				catch (IOException e) 
				{
					e.printStackTrace();
				}
				// sleep a short amount of time
				Thread.sleep(this.random.nextInt(100));
			} 
			catch (InterruptedException e) 
			{
				// do nothing
			}
		}
	}
	
	/**
	 * Returns the performed modifications of this thread.
	 * 
	 * @return The modifications performed by this thread.
	 */
	public HashMap<Integer, Integer> getModifiedList()
	{
		return this.modifiedValues;
	}
	
	/**
	 * Modifies the binary page by increasing the stored value. 
	 * 
	 * @param binaryEncoded The binary page containing the field.
	 * @param offset The offset where to store the field.
	 */
	private void manipulateBinaryPage(byte[] binaryEncoded, int offset)
	{
		// synchronize on binary page
		synchronized(binaryEncoded)
		{
			int fieldValue = IntField.getFieldFromBinary(binaryEncoded, offset).getValue();
			fieldValue++;
			IntField.encodeIntAsBinary(fieldValue, binaryEncoded, offset);
			this.manipulations++;
			if(this.manipulations == totalManipulations)
			{
				this.alive = false;
			}
		}
	}
}
