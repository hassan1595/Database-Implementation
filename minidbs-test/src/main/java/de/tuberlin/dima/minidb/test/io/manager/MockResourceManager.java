package de.tuberlin.dima.minidb.test.io.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.manager.ResourceManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;

/**
 * Mock implementation of a resource manager to get more 
 * more information about the undergoing processes of the
 * Buffer Pool Manager.
 *
 * @author Michael Saecker
 */
public class MockResourceManager extends ResourceManager 
{
	/**
	 * The next page number for a newly created page.
	 */
	private int pageNumber;

	/**
	 * The creator of this ResourceManager.
	 */
	private Thread creator;
	
	/**
	 * The page size of this resource.
	 */
	private PageSize pageSize;
	
	/**
	 * The schema of this resource.
	 */
	private TableSchema schema;
	
	/**
	 * A list of byte arrays simulating the disk.
	 */
	private ArrayList<byte[]> diskBuffer;
	
	/**
	 * The thread that was identified as issuing read requests to this resource manager. 
	 */
	private Thread reader;
	
	/**
	 * The thread that was identified as issuing write requests to this resource manager. 
	 */
	private Thread writer;
	
	/**
	 * The list of received read requests.
	 */
	private LinkedList<Integer> readRequests;
	
	/**
	 * The list of received write requests.
	 */
	private LinkedList<Integer> writeRequests;
	
	/**
	 * Flag indicating whether a delay for reading should be introduced to measure prefetching.
	 */
	private final boolean prefetch;
	
	/**
	 * Error messages.
	 */
	public static final String readingSeparateThread = "Reading should be done by a separate thread.";
	public static final String readingSameThread = "Reading should be done by the same thread.";
	public static final String writingSeparateThread = "Writing should be done by a seperate thread.";
	public static final String writingSameThread = "Writing should be done by the same thread.";
	
	private static final int firstPageNumber = 1;
	
	/**
	 * Creates a mock resource manager.
	 * 
	 * @param schema The schema to use.
	 * @param pageSize The page size to use.
	 */
	public MockResourceManager(TableSchema schema, PageSize pageSize, boolean prefetch) 
	{
		this.pageNumber = firstPageNumber;
		this.creator = Thread.currentThread();
		this.pageSize = pageSize;
		this.schema = schema;
		this.diskBuffer = new ArrayList<byte[]>();
		this.writer = null;
		this.reader = null;
		this.readRequests = new LinkedList<Integer>();
		this.writeRequests = new LinkedList<Integer>();
		this.prefetch = prefetch;
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#getPageSize()
	 */
	@Override
	public PageSize getPageSize() 
	{
		return this.pageSize;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#truncate()
	 */
	@Override
	public void truncate() 
		throws IOException 
	{
		this.diskBuffer.clear();
		this.pageNumber = firstPageNumber;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#closeResource()
	 */
	@Override
	public void closeResource() 
		throws IOException 
	{
		// do nothing
		this.truncate();
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#readPageFromResource(byte[], int)
	 */
	@Override
	public CacheableData readPageFromResource(byte[] buffer, int pageNumber)
			throws IOException 
	{
		// add the request to the list of read requests
		this.readRequests.add(pageNumber);
		
		// check if thread is different than the creator
		if (Thread.currentThread() == this.creator)
		{
			throw new IOException(readingSeparateThread);
		}
		// check if reader thread is already set
		if (this.reader == null)
		{
			this.reader = Thread.currentThread();
		}
		// check if it is always the same reading thread
		if (this.reader != Thread.currentThread())
		{
			throw new IOException(readingSameThread);
		}
		// generate page to return
		TablePage page = null;
		if (pageNumber < this.pageNumber && pageNumber >= firstPageNumber)
		{
			// read contents from "disk"
			System.arraycopy(this.diskBuffer.get(pageNumber - 1), 0, buffer, 0, buffer.length);
			try 
			{
				page = AbstractExtensionFactory.getExtensionFactory().createTablePage(this.schema, buffer);
			} 
			catch (Exception e) {
				throw new IOException("Error initializing page: " + e.getMessage());
			}
		}
		else
		{
			throw new IOException("This page does not exist.");
		}
		// wait to test prefetching
		if(this.prefetch)
		{
			try 
			{
				Thread.sleep(200);
			} 
			catch (InterruptedException e) 
			{
				// do nothing
			}
		}
		
		return page;
	}

	@Override
	public CacheableData[] readPagesFromResource(byte[][] buffers, int firstPageNumber) throws IOException {
		CacheableData[] pages = new CacheableData[buffers.length];
		for (int i = 0; i < buffers.length; i++) {
			pages[i] = readPageFromResource(buffers[i], firstPageNumber+i);
		}
		return pages;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#writePageToResource(byte[], de.tuberlin.dima.minidb.io.cache.CacheableData)
	 */
	@Override
	public void writePageToResource(byte[] buffer, CacheableData wrapper)
			throws IOException 
	{
		this.writeRequests.add(wrapper.getPageNumber());
		
		// check if thread is different than the creator
		if (Thread.currentThread() == this.creator)
		{
			System.out.println(writingSeparateThread);
			throw new IOException(writingSeparateThread);
		}
		// check if writer thread is already set
		if (this.writer == null)
		{
			this.writer = Thread.currentThread();
		}
		// check if it is always the same writing thread
		if (this.writer != Thread.currentThread())
		{
			System.out.println(writingSameThread);
			throw new IOException(writingSameThread);
		}		
		// write contents to specified position
		if(wrapper.getPageNumber() < this.pageNumber && wrapper.getPageNumber() >= firstPageNumber)
		{
			// copy contents to "disk"
			System.arraycopy(buffer, 0, this.diskBuffer.get(wrapper.getPageNumber() - 1), 0, buffer.length);
		}
		else
		{
			throw new IOException("This page does not exist.");
		}
	}

	@Override
	public void writePagesToResource(byte[][] buffers, CacheableData[] wrappers) throws IOException {
		for (int i = 0; i < buffers.length; i++) {
			writePageToResource(buffers[i], wrappers[i]);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[])
	 */
	@Override
	public CacheableData reserveNewPage(byte[] ioBuffer) throws IOException,
			PageFormatException 
	{
		// create disk representation of data
		byte[] diskCopy = new byte[this.pageSize.getNumberOfBytes()];
		// copy current contents of buffer to disk array
		System.arraycopy(ioBuffer, 0, diskCopy, 0, ioBuffer.length);
		TablePage page = AbstractExtensionFactory.getExtensionFactory().initTablePage(this.schema, ioBuffer, this.pageNumber++);
		this.diskBuffer.add(diskCopy);
		return page;
	}

	/*
	 * (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.io.ResourceManager#reserveNewPage(byte[], java.lang.Enum)
	 */
	@Override
	public CacheableData reserveNewPage(byte[] ioBuffer, Enum<?> type)
			throws IOException, PageFormatException 
	{
		return reserveNewPage(ioBuffer);
	}
	

	/**
	 * Returns a clone of the list of write requests issued to this resource manager. 
	 * 
	 * @return The list of write requests.
	 */
	@SuppressWarnings("unchecked")
	public LinkedList<Integer> getWriteRequests()
	{
		return (LinkedList<Integer>) this.writeRequests.clone();
	}
	
	/**
	 * Returns a clone of the list of read requests issued to this resource manager. 
	 * 
	 * @return The list of read requests.
	 */
	@SuppressWarnings("unchecked")
	public LinkedList<Integer> getReadRequests()
	{
		return (LinkedList<Integer>) this.readRequests.clone();
	}
}
