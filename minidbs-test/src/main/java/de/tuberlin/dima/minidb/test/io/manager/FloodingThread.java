package de.tuberlin.dima.minidb.test.io.manager;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import de.tuberlin.dima.minidb.io.manager.BufferPoolException;

/**
 * Class flooding the buffer pool manager with arbitrary
 * requests for the given resource.
 *
 * @author Michael Saecker
 */
public class FloodingThread extends Thread 
{
	
	/**
	 * The owner of this thread.
	 */
	private AbstractTestBufferPoolManager parent;
	
	/**
	 * Flag indicating whether this thread should continue working.
	 */
	private volatile boolean alive;
	
	/**
	 * The resource for which to send requests.
	 */
	private int resource;
	
	/**
	 * The list of page numbers the resource contains.
	 */
	private List<Integer> pageNumbers;
	
	/**
	 * The random number generator for this thread.
	 */
	private Random random;
	
	/**
	 * Creates the thread that keeps sending requests to the bpm.
	 * 
	 * @param parent The owner of this thread.
	 * @param resourceId The resource to query.
	 * @param pageNumbers The page numbers of the resource.
	 */
	public FloodingThread(AbstractTestBufferPoolManager parent, int resourceId, List<Integer> pageNumbers)
	{
		this.parent = parent;
		this.alive = true;
		this.resource = resourceId;
		this.pageNumbers = pageNumbers;
		this.random = new Random();
	}

	/**
	 * Execution code of the thread.
	 * (Request pages)
	 */
	@Override
	public void run()
	{
		while (this.alive)
		{
			// query generated pages
			try 
			{
				int pageNumber = this.pageNumbers.get(this.random.nextInt(this.pageNumbers.size()));
				this.parent.underTest.getPageAndPin(this.resource, pageNumber);
				this.parent.underTest.unpinPage(this.resource, pageNumber);
				// make sure to have frequent pages
				if (this.random.nextBoolean())
				{
					// re-hit page
					this.parent.underTest.getPageAndPin(this.resource, pageNumber);
					this.parent.underTest.unpinPage(this.resource, pageNumber);					
				}
				try 
				{
					Thread.sleep(this.random.nextInt(50));
				} 
				catch (InterruptedException e) 
				{
					// do nothing
				}
			} 
			catch (BufferPoolException e) 
			{
				e.printStackTrace();
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * Stops the execution of this thread 
	 * by setting the alive flag to false.
	 */
	public void stopThread()
	{
		this.alive = false;
	}
	
}
