package de.tuberlin.dima.minidb.test.io.cache;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.CachePinnedException;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.DuplicateCacheEntryException;
import de.tuberlin.dima.minidb.io.cache.EvictedCacheEntry;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageSize;


/**
 * The public test case for the cache.
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 */
public class TestPageCacheStudents
{
	/**
	 * Fixed seed to make tests reproducible. Teacher test uses a different seed so please try your solution with another seed to check for remaining errors.
	 */
	private static final long SEED = 347987672876524L;
		
	/**
	 * The random number generator used to create elements.
	 */
	private Random random = new Random(SEED);
	
	private PageCache underTest;
	private TableSchema schema;
	private Map<EntryId, CacheableData> contained;	
	
	/**
	 * Sets up a warm cache (all elements hit only once).
	 * Also tests the addPage() behavior for cold caches.
	 * 
	 * @see junit.framework.TestCase#setUp()
	 */
	@Before
	public void setUp() throws Exception
	{
		// load the custom code
		AbstractExtensionFactory.initializeDefault();
		
		// even number size [500 .. 2000]
		int size = this.random.nextInt(750) * 2 + 502;
		// create a page size, but exclude the last one (consumes too much memory)
		// to avoid OutOfMemoryErrors. Note: With higher -xmx parameter that would work
		PageSize pz = PageSize.values()[this.random.nextInt(PageSize.values().length - 1)];
		this.schema = initSchema(pz);
		
		// create the test instance
		this.underTest = AbstractExtensionFactory.getExtensionFactory().createPageCache(pz, size);
		this.contained = new HashMap<EntryId, CacheableData>(size);
		byte[] buffer = new byte[pz.getNumberOfBytes()];

		// fill empty cache with data and check that it behaves properly as a cold cache does
        while (this.contained.size() < size) {
			
			// generate the next entry
            int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData next = null;
			EntryId eid = null;
			do {
				next = generateRandomCacheEntry(resourceId, buffer);
				eid = new EntryId(resourceId, next.getPageNumber());
			}
			while (this.contained.containsKey(eid));
			this.contained.put(eid, next);
			
			// add the entry and make sure we always get the empty entries back and no
			// others
			try {
				EvictedCacheEntry evicted = this.underTest.addPage(next, eid.getResourceId());
				assertTrue("addPage() method must not return null.", evicted != null);
				assertTrue("Initial entries must contain a buffer", evicted.getBinaryPage() != null);
				assertTrue("Initial entries must have no resource data.", evicted.getWrappingPage() == null);
				// disable prefetching behavior (request page once)
				this.underTest.getPage(eid.getResourceId(), eid.getPageNumber());
				
				// recycle the buffer
				buffer = evicted.getBinaryPage();
				assertTrue(buffer != null);
			}
			catch (DuplicateCacheEntryException sceex) {
				fail("No duplicate entry occurred here, no exception must be thrown.");
			}
			catch (CachePinnedException cpex) {
				fail("Cache is not pinned at this point.");
			}
		}
	}
	
	/**
	 * Cleanup after a test case.
	 */
	@After
    public void tearDown() throws Exception
    {
	    this.underTest = null;
	    this.contained.clear();
	    System.gc();
    }


	/**
	 * Tests whether the correct capacity is returned.
	 */
	@Test
	public void testGetCapacity()
	{
		assertTrue("Capacity must be tracked correctly.", this.underTest.getCapacity() == this.contained.size());
	}
	
	/**
	 * Tests fetching tables from the cache.
	 */
	@Test
	public void testGetPage() throws Exception
	{
		// simply test that all contained pages can be fetched from the cache
		
		
		Iterator<EntryId> iter = this.contained.keySet().iterator();
		
		while (iter.hasNext())
		{
			EntryId nextEntry = iter.next();
			CacheableData ret = this.underTest.getPage(nextEntry.getResourceId(), nextEntry.getPageNumber());
			assertTrue("Entry could not be retrieved from the cache.", ret != null);
			assertTrue("Resource is incorrectly identified.",
						nextEntry.getPageNumber() == ret.getPageNumber());
			assertTrue("Cache should not invalidate a regular entry.", !ret.isExpired());
			
			// check the byte buffer marking
			int hashcode = nextEntry.hashCode();
			int val = IntField.getIntFromBinary(ret.getBuffer(), this.schema.getPageSize().getNumberOfBytes() - 4);
			assertTrue("byte buffer has been switched while data was in cache.", val == hashcode);
		}
		
		// now check that null is returned for not contained entries
		for (int i = 0; i < 1000; i++)
		{
			EntryId id = generateRandomEntryId();
			if (this.contained.containsKey(id)) {
				continue;
			}
			
			assertTrue("A not contained entry must return null.", this.underTest.getPage(id.getResourceId(), id.getPageNumber()) == null);
		}
	}
	
	
	/**
	 * Tests adding pages to the cache.
	 */
	@Test
	public void testAddPage() throws Exception
	{
		// make sure that a duplicate entry exception is thrown
		Iterator<EntryId> iter = this.contained.keySet().iterator();
		
		while (iter.hasNext())
		{
			try 
			{
				EntryId nextEntry = iter.next(); 
				CacheableData cd = this.contained.get(nextEntry);
				this.underTest.addPage(cd, nextEntry.getResourceId());
				
				fail("An exception should have been thrown here.");
			}
			catch (DuplicateCacheEntryException dceex) 
			{
				// we are cool
			}
		}
	}
	
	
	/**
	 * Tests the cache behavior assuming ARC implementation. 
	 */
	@Test
	public void testCacheBehavior() throws Exception
	{		
		Set<EntryId> newlyReferenced = new HashSet<EntryId>(this.underTest.getCapacity() / 2);
		
		// get a set of contained entries and reference them multiple times (move all pages from t1 to t2)
		Iterator<EntryId> iter = this.contained.keySet().iterator();
		for (int i = 0; i < this.contained.size() / 2; i++)
		{
			EntryId entry = iter.next();
			
			assertTrue("Element not contained though it should be.", this.underTest.getPage(entry.getResourceId(), entry.getPageNumber()) != null);
			newlyReferenced.add(entry);
		}
		
		// should not change ARC behavior status compared to last request run (all pages still in t2)
		iter = newlyReferenced.iterator();
		while (iter.hasNext()) {
			EntryId entry = iter.next();
			assertTrue("Element not contained though it should be.", this.underTest.getPage(entry.getResourceId(), entry.getPageNumber()) != null);
		}
		
		// now, add some new and see that none of the newly referenced is evicted
		Set<EntryId> randomlyAdded = new HashSet<EntryId>(this.underTest.getCapacity() / 2);
		byte[][] stored_buffers = new byte[newlyReferenced.size()][this.schema.getPageSize().getNumberOfBytes()];
		byte[] buffer = new byte[this.schema.getPageSize().getNumberOfBytes()];
		for (int i = 0; i < this.contained.size() / 2; i++)
		{
			int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData newEntry = generateRandomCacheEntry(resourceId, buffer);
			EntryId entry = new EntryId(resourceId, newEntry.getPageNumber());
			randomlyAdded.add(entry);
			try {
				EvictedCacheEntry evicted = this.underTest.addPage(newEntry, resourceId);
				// access page to disable prefetching behavior options
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				// recycle buffer
				buffer = evicted.getBinaryPage();
				stored_buffers[i] = buffer;
				assertTrue(buffer != null);
				assertTrue("None of the newly referenced ones should have been evicted.", !newlyReferenced.contains(new EntryId(evicted.getResourceID(), evicted.getPageNumber())));
			}
			catch (DuplicateCacheEntryException dceex) 
			{
				// random test generated duplicate, just ignore it
				// and add another tuple
				i--;
			}
		}
		
		// access randomly accessed pages again to move them to frequent cache and evict newly referenced ones from there 
		iter = randomlyAdded.iterator();
		while (iter.hasNext()) {
			EntryId entry = iter.next();
			assertTrue("Page must be contained in cache.", this.underTest.getPage(entry.getResourceId(), entry.getPageNumber()) != null);
		}

		// again, add some new and see that xy of the newly referenced are evicted
		randomlyAdded = new HashSet<EntryId>(this.underTest.getCapacity() / 2);
		stored_buffers = new byte[newlyReferenced.size()][this.schema.getPageSize().getNumberOfBytes()];
		buffer = new byte[this.schema.getPageSize().getNumberOfBytes()];
		for (int i = 0; i < this.contained.size() / 2; i++)
		{
			int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData newEntry = generateRandomCacheEntry(resourceId, buffer);
			EntryId entry = new EntryId(resourceId, newEntry.getPageNumber());
			randomlyAdded.add(entry);
			try {
				EvictedCacheEntry evicted = this.underTest.addPage(newEntry, resourceId);
				// access page to disable prefetching behavior options
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				// access again to keep t1 list empty and move newly referenced items to b2
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				// recycle buffer
				buffer = evicted.getBinaryPage();
				stored_buffers[i] = buffer;
				assertTrue(buffer != null);
				assertTrue("All of the newly referenced ones should have been evicted.", newlyReferenced.contains(new EntryId(evicted.getResourceID(), evicted.getPageNumber())));
			}
			catch (DuplicateCacheEntryException dceex) {
				// random test generated duplicate, just ignore it
				// and add another tuple
				i--;
			}
		}		
		
		// at this point, all newly referenced pages should be in bottom list of frequent portion
		int n = 0;
		iter = newlyReferenced.iterator();
		while (iter.hasNext()) {
			EntryId entry = iter.next();
			this.underTest.getPage(entry.getResourceId(), entry.getPageNumber());
			assertTrue("All formerly newly referenced pages should now be in bottom list and thus not cached.", this.underTest.getPage(entry.getResourceId(), entry.getPageNumber()) == null);
			CacheableData newEntry = AbstractExtensionFactory.getExtensionFactory().initTablePage(this.schema, stored_buffers[n], entry.getPageNumber());
			this.contained.put(entry, newEntry);
			this.underTest.addPage(newEntry, entry.getResourceId());
			n++;
		}
		
		// now newly referenced pages should be in t2 (pages moved from b2 to t2) / refresh pages in t2
		// add cache size number of items to simulate scan
		randomlyAdded = new HashSet<EntryId>(this.underTest.getCapacity());
		stored_buffers = new byte[this.contained.size()][this.schema.getPageSize().getNumberOfBytes()];
		buffer = new byte[this.schema.getPageSize().getNumberOfBytes()];
		for (int i = 0; i < this.contained.size(); i++)
		{
			int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData newEntry = generateRandomCacheEntry(resourceId, buffer);
			EntryId entry = new EntryId(resourceId, newEntry.getPageNumber());
			randomlyAdded.add(entry);
			try {
				EvictedCacheEntry evicted = this.underTest.addPage(newEntry, resourceId);
				// access page to disable prefetching behavior options
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				// recycle buffer
				buffer = evicted.getBinaryPage();
				stored_buffers[i] = buffer;
				assertTrue(buffer != null);
			}
			catch (DuplicateCacheEntryException dceex) {
				// random test generated duplicate, just ignore it
				// and add another tuple
				i--;
			}
		}
				
		// check that pages are in still in cache (were added to frequent and not to recent list)
		iter = newlyReferenced.iterator();
		while (iter.hasNext()) {
			EntryId entry = iter.next();
			this.underTest.getPage(entry.getResourceId(), entry.getPageNumber());
			assertTrue("Page must be contained in cache.", this.underTest.getPage(entry.getResourceId(), entry.getPageNumber()) != null);
		}
	}
	
	/**
	 * Tests the pinning of pages in the cache.
	 */
	@Test
	public void testPinning() throws Exception
	{
		Set<EntryId> pinned = new HashSet<EntryId>(this.contained.size() / 2);
		
		// first, get half of the elements and pin them
		Iterator<EntryId> iter = this.contained.keySet().iterator();
		
		for (int i = 0; i < this.contained.size() / 2; i++) 
		{
			EntryId e = iter.next();
			assertTrue("getAndPin() returned wrong entry.", 
				this.contained.get(e) == this.underTest.getPageAndPin(e.getResourceId(), e.getPageNumber()));
			pinned.add(e);
		}
		
		// now add a lot of other entries and make sure that the pinned do not get evicted
		byte[] buffer = new byte[this.schema.getPageSize().getNumberOfBytes()];
		for (int i = 0; i < this.contained.size() * 10; i++)
		{
			int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData newEntry = generateRandomCacheEntry(resourceId, buffer);
			
			try {
				EvictedCacheEntry e = this.underTest.addPage(newEntry, resourceId);
				// go against prefetching behavior
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				if(this.random.nextBoolean()) // only move some to t2
				{
					// fetch again so that they move to frequent buffer
					this.underTest.getPage(resourceId, newEntry.getPageNumber());
				}
				assertTrue(e != null);
				
				// recycle buffer
				buffer = e.getBinaryPage();
				assertTrue(buffer != null);
				assertFalse("None of the pinned ones should have been evicted.",
						pinned.contains(new EntryId(e.getResourceID(), e.getPageNumber())));
			}
			catch (DuplicateCacheEntryException dceex) {
				// random test generated duplicate, just ignore it 
			}
		}
		
		// unpin the old ones
		Iterator<EntryId> i2 = pinned.iterator();
		while (i2.hasNext()) {
			EntryId next = i2.next();
			this.underTest.unpinPage(next.getResourceId(), next.getPageNumber());
		}
		
		// add a load of new ones and make sure the pinned ones get also evicted
		for (int i = 0; i < this.contained.size() * 10; i++)
		{
			int resourceId = this.random.nextInt(Integer.MAX_VALUE);
			CacheableData newEntry = generateRandomCacheEntry(resourceId, buffer);
			
			try {
				EvictedCacheEntry e = this.underTest.addPage(newEntry, resourceId);
				assertTrue(e != null);
				// work against prefetching behavior
				this.underTest.getPage(resourceId, newEntry.getPageNumber());
				if(this.random.nextBoolean()) // only move some to t2
				{
					// move pages into frequent list by fetching them anew
					this.underTest.getPage(resourceId, newEntry.getPageNumber());
				}
				
				// recycle buffer
				buffer = e.getBinaryPage();
				assertTrue(buffer != null);
				pinned.remove(new EntryId(e.getResourceID(), e.getPageNumber()));
			}
			catch (DuplicateCacheEntryException dceex) {
				// random test generated duplicate, just ignore it 
			}
		}
		
		// the pinned set should be empty by now
		assertTrue("No pinned ones should remain.", pinned.isEmpty());
		
		// unpin a non existing page
		EntryId eid = null; 
		do
		{
			eid = new EntryId(this.random.nextInt(), this.random.nextInt());
		}
		while(this.contained.containsKey(eid));
		this.underTest.unpinPage(eid.getResourceId(), eid.getPageNumber());
		
		// unpin an existing but unpinned page
		eid = this.contained.keySet().iterator().next();
		this.underTest.unpinPage(eid.getResourceId(), eid.getPageNumber());
	}
	
	/**
	 * Tests expelling pages of a certain resource from the cache.
	 */
	@Test
	public void testRetrievingExpellingAll()
	throws Exception
	{
		// test a lot or nothing, who cares anyways... (get all / expell all)
		final int id = 155; // resource id
		
		Set<CacheableData> res = new HashSet<CacheableData>(this.contained.size() / 2);
		byte[] buffer = new byte[this.schema.getPageSize().getNumberOfBytes()];
		
		for (int i = 0; i < this.contained.size() / 2; i++) {
			CacheableData n = AbstractExtensionFactory.getExtensionFactory().initTablePage(this.schema, buffer, i+3);
			
			try {
				EvictedCacheEntry o = this.underTest.addPage(n, id);
				// work against prefetching behavior
				if(this.random.nextBoolean()) // randomly pin pages
				{
					this.underTest.getPage(id, n.getPageNumber());
				}
				else
				{
					this.underTest.getPageAndPin(id, n.getPageNumber());
				}
				assertTrue(o != null);
				buffer = o.getBinaryPage();
				assertTrue(buffer != null);
				res.add(n);
			}
			catch (DuplicateCacheEntryException dceex) {
				// yo, never mind
			}
		}
		
		// test that we never return null (moves pages to t2)
		for (int i = 0; i < 77; i++)
		{
			assertTrue("getAllPagesForResource() must never return null.", this.underTest.getAllPagesForResource(i) != null);
		}
		
		// get the results for our resource
		CacheableData[] allHits = this.underTest.getAllPagesForResource(id);
		assertTrue(allHits != null);
		assertTrue("Too few hits found for getAllPagesForResource().", allHits.length >= this.contained.size() / 2);
		
		// first make sure that all that we added in the function was also found
		Set<CacheableData> hits = new HashSet<CacheableData>(Arrays.asList(allHits));
		Iterator<CacheableData> iter = res.iterator();
		while (iter.hasNext()) {
			assertTrue("Resource was not returned by getAllPagesForResource().", hits.contains(iter.next()));  
		}
		// check that the lists are identical
		iter = hits.iterator();
		while(iter.hasNext())
		{
			assertTrue("Resource was not returned by getAllPagesForResource().", res.contains(iter.next()));
		}		
		// expell them all
		this.underTest.expelAllPagesForResource(id);
		
		// make sure that they are the next ones and invalid
		for (int i = 0; i < this.contained.size() / 2; i++) 
		{
			CacheableData ce = AbstractExtensionFactory.getExtensionFactory().initTablePage(this.schema, buffer, i + this.contained.size() + id);
			try 
			{
				EvictedCacheEntry e = this.underTest.addPage(ce, id+i);
				// work against prefetching behavior
				this.underTest.getPage(id+i, ce.getPageNumber());
				assertTrue(e != null);
				buffer = e.getBinaryPage();
				assertTrue(buffer != null);
				assertTrue("Expelled page must belong to resource " + id, e.getResourceID() == id);
				assertFalse("The expelled entry must be valid.", e.getWrappingPage().isExpired());
			}
			catch (DuplicateCacheEntryException deex) {
				// retry
				i--;
			}
		}
	}
	
	/*
	 * ********************************************************************************************
	 * 
	 *                                       U t i l i t i e s
	 *                    
	 * ********************************************************************************************
	 */
	
	/**
	 * Generates a random entry id. (Helper class for test case).
	 */
	private EntryId generateRandomEntryId()
	{
		int id = this.random.nextInt(Integer.MAX_VALUE);
		int num = this.random.nextInt(Integer.MAX_VALUE);
		
		return new EntryId(id, num);
	}

	
	/**
	 * Generates a random cache entry.
	 * 
	 * @param id The resource id of the page.
	 * @param buffer The buffer to use for the page.
	 * @return The cache entry.
	 * @throws Exception
	 */
	private CacheableData generateRandomCacheEntry(int id, byte[] buffer) throws Exception
	{		
		int num = this.random.nextInt(Integer.MAX_VALUE);
		
		// generate a dummy page around it
		CacheableData cd = AbstractExtensionFactory.getExtensionFactory().initTablePage(this.schema, buffer, num);
		
		// mark the data buffer
		IntField.encodeIntAsBinary(new EntryId(id, num).hashCode(), buffer, this.schema.getPageSize().getNumberOfBytes() - 4);
		
		return cd;
	}
	
	/**
	 * Initializes a schema for the given page size.
	 * 
	 * @param pageSize The page size.
	 * @return The schema.
	 */
	private TableSchema initSchema(PageSize pageSize)
	{
		TableSchema schema = new TableSchema(pageSize);
		
		// generate a random set of columns
		int numCols = this.random.nextInt(20) + 1;
		
		// create the columns as given
		for (int col = 0; col < numCols; col++) {
			DataType type = getRandomDataType();
			schema.addColumn(ColumnSchema.createColumnSchema("Random Column " + col, type, true));
		}
		
		return schema;
	}
	
	
	/**
	 * Generates a random data type for the schema.
	 * 
	 * @return A random data type.
	 */
	private DataType getRandomDataType()
	{
		int num = this.random.nextInt(10);
		
		switch (num) {
		case 0:
			return DataType.smallIntType();
		case 1:
			return DataType.intType();
		case 2:
			return DataType.bigIntType();
		case 3:
			return DataType.floatType();
		case 4:
			return DataType.doubleType();
		case 5:
			return DataType.charType(this.random.nextInt(256) + 1);
		case 6:
			return DataType.varcharType(this.random.nextInt(256) + 1);
		case 7:
			return DataType.dateType();
		case 8:
			return DataType.timeType();
		case 9:
			return DataType.timestampType();
		default:
			return DataType.intType();	
		}
	}	

	
	
	/**
	 * Helper class to track resource ids and page numbers of cache entries.
	 */
	private class EntryId
	{
		private int id;
		private int pagenumber;
		

		/**
		 * Ctr.
		 * 
		 * @param id The resource id.
		 * @param pagenumber The page number.
		 */
		public EntryId(int id, int pagenumber)
        {
	        this.id = id;
	        this.pagenumber = pagenumber;
        }


		/**
		 * Returns the page number of this entry id.
		 * 
		 * @return The page number.
		 */
		public int getPageNumber() 
		{
			return this.pagenumber;
		}


		/**
		 * Returns the resource id of this entry id.
		 * 
		 * @return The resource id.
		 */
		public int getResourceId() 
		{
			return this.id;
		}


		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
        public int hashCode()
        {
	        final int prime = 31;
	        int result = 1;
	        result = prime * result + this.id;
	        result = prime * result + this.pagenumber;
	        return result;
        }

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
        public boolean equals(Object obj)
        {
	        if (this == obj)
		        return true;
	        if (obj == null)
		        return false;
	        if (getClass() != obj.getClass())
		        return false;
	        EntryId other = (EntryId) obj;
	        if (this.id != other.id)
		        return false;
	        if (this.pagenumber != other.pagenumber)
		        return false;
	        return true;
        }		
	}
}
