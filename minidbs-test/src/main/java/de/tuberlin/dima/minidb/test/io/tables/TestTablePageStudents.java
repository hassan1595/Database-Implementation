package de.tuberlin.dima.minidb.test.io.tables;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.BigIntField;
import de.tuberlin.dima.minidb.core.CharField;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataFormatException;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.DateField;
import de.tuberlin.dima.minidb.core.DoubleField;
import de.tuberlin.dima.minidb.core.FloatField;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.core.SmallIntField;
import de.tuberlin.dima.minidb.core.TimeField;
import de.tuberlin.dima.minidb.core.TimestampField;
import de.tuberlin.dima.minidb.core.VarcharField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TupleIterator;
import de.tuberlin.dima.minidb.io.tables.TupleRIDIterator;
import de.tuberlin.dima.minidb.parser.Predicate.Operator;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.util.Pair;


/**
 * The public test case for the table pages.
 * 
 * @author Stephan Ewen (sewen@cs.tu-berlin.de)
 * @author Michael Saecker
 */
public class TestTablePageStudents
{
	
	/**
	 * Fixed seed to make tests reproducible. Teacher test uses a different seed so please try your solution with another seed to check for remaining errors.
	 */
	private static final long SEED = 347987672876524L;
	
	/**
	 * Number of schemas to generate.
	 */
	private static final int NUM_SCHEMAS = 16;
	
	/**
	 * Array holding all schemas for different page sizes.
	 */
	private static TableSchema[][] schemas;
	
	/**
	 * Array holding all page sizes.
	 */
	private static Integer[] pageSizes;
	
	/**
	 * Array holding all max widths for records for the different schemas and page sizes.
	 */
	private static Integer[][] totalMaxWidths;

	/**
	 * The random number generator used to generate data.
	 */
	private static Random random;
	
	/**
	 * Static initialization for all test cases.
	 */
	static 
	{
		pageSizes = new Integer[]{ 4096, 16384, 8192 }; 
		schemas = new TableSchema[pageSizes.length][NUM_SCHEMAS];
		totalMaxWidths = new Integer[pageSizes.length][NUM_SCHEMAS];		
		// initialize the random number generator
		random = new Random(SEED);
		for(int i = 0; i < pageSizes.length; i++)
		{
			generateSchemas(pageSizes[i], i, schemas, totalMaxWidths);
		}
	}
	
	@Before
	public void setUp() throws Exception
	{
		// load the custom code
		AbstractExtensionFactory.initializeDefault();	
	}
	
	@After
	public void tearDown() throws Exception
	{
		
	}

	
	/**
	 * Tests whether the page handles retrieval of full records correctly. 
	 */
	@Test
	public void testRetrievalOfFullTuples() throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();
				
				// do tests
				// fetch and check against the list (all columns of record)
				for (int i = 0; i < list.size(); i++) {
					DataTuple orig = list.get(i);
					DataTuple inPage = page.getDataTuple(i, Long.MAX_VALUE, numCols);
					assertTrue("Tuples must be equal:\n" + orig.toString() + "\n" + inPage.toString()
							, orig.equals(inPage));
				}				
				// help the garbage collector
				list.clear();				
			}
		}
	}

	/**
	 * Tests whether the page handles projection correctly. 
	 */
	@Test
	public void testRetrievalWithProjection() throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				// fetch and check against the list (select only specific columns)
				TreeSet<Integer> columnsToFetch = new TreeSet<Integer>();
				for(int i = 0; i < Math.floor(numCols/2); i++)
				{
					Integer a = 0;
					do 
					{
						a = random.nextInt(numCols);
					}
					while(columnsToFetch.contains(a));
					columnsToFetch.add(a);
				}
				// build column bitmap
				long columnBitmap = buildColumnBitmap(numCols, columnsToFetch);
				long tmpColumnBitmap = columnBitmap;
				for (int i = 0; i < list.size(); i++)
				{
					DataTuple orig = list.get(i);
					DataTuple origCmp = new DataTuple(columnsToFetch.size());
					int colPos = 0;
					// create tuple from the list containing only the required columns
					for (int x = 0; x < numCols; x++)
					{
						if((tmpColumnBitmap & 0x00000001) == 0x1)
						{
							// add column
							origCmp.assignDataField(orig.getField(x), colPos++);
						}
						tmpColumnBitmap >>>= 1;
					}
					DataTuple inPage = page.getDataTuple(i, columnBitmap, columnsToFetch.size());
					assertTrue("The extracted tuples with filtered columns must be equal:\n" +
							origCmp.toString() + "\n" + inPage.toString(), origCmp.equals(inPage));
					// reset bitmap to create tuples from list
					tmpColumnBitmap = columnBitmap;
				}				
				// help the garbage collector
				list.clear();								
			}
		}	
	}
	
	/**
	 * Tests whether the page handles predicates correctly. 
	 */
	@Test
	public void testRetrievalWithPredicates()
	throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				// fetch and check against the list (all columns of record with predicats)
				LowLevelPredicate[] predicates = generatePredicates(schema, numCols); 
				for (int i = 0; i < list.size(); i++) 
				{
					boolean origPasses = true;
					DataTuple orig = list.get(i);
					DataTuple inPage = page.getDataTuple(predicates, i, Long.MAX_VALUE, numCols);
					for(int x = 0; x < predicates.length; x++)				{
						if (!predicates[x].evaluateWithNull(orig.getField(predicates[x].getColumnIndex())))
						{
							origPasses = false;
						}
					}
					if(origPasses)
					{
						assertTrue("The tuples should be equal.", orig.equals(inPage));
					}
					else
					{
						assertTrue("Orginal did not pass predicates, so the table page should return null.", inPage == null);
					}
				}						
				// help the garbage collector
				list.clear();				
			}
		}		
	}
	
	/**
	 * Tests whether the page handles projection and predicates correctly. 
	 */
	@Test
	public void testRetrievalWithProjectionAndPredicates()
	throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				// fetch and check against the list (select only specific columns with predicates)
				// select columns to fetch
				TreeSet<Integer> columnsToFetch = new TreeSet<Integer>();
				for(int i = 0; i < Math.floor(numCols/2); i++)
				{
					Integer a = 0;
					do 
					{
						a = random.nextInt(numCols);
					}
					while(columnsToFetch.contains(a));
					columnsToFetch.add(a);
				}
				
				// build column bitmap
				LowLevelPredicate[] predicates = generatePredicates(schema, numCols); 
				long columnBitmap = buildColumnBitmap(numCols, columnsToFetch);
				long tmpColumnBitmap = columnBitmap;
				for (int i = 0; i < list.size(); i++)
				{
					boolean origPasses = true;
					DataTuple orig = list.get(i);
					DataTuple origCmp = new DataTuple(columnsToFetch.size());
					int colPos = 0;
					for (int x = 0; x < numCols; x++)
					{
						if((tmpColumnBitmap & 0x00000001) == 0x1)
						{
							// add column
							origCmp.assignDataField(orig.getField(x), colPos++);
						}
						tmpColumnBitmap >>>= 1;
					}
					DataTuple inPage = page.getDataTuple(predicates, i, columnBitmap, columnsToFetch.size());
					for(int x = 0; x < predicates.length; x++)
					{
						if (!predicates[x].evaluateWithNull(orig.getField(predicates[x].getColumnIndex())))
						{
							origPasses = false;
						}
					}
					if(origPasses)
					{
						assertTrue("The tuples should be equal.", origCmp.equals(inPage));
					}
					else
					{
						assertTrue("Orginal did not pass predicates, so the table page should return null.", inPage == null);
					}
					// reset bitmap to create tuples from list
					tmpColumnBitmap = columnBitmap;
				}							
				// help the garbage collector
				list.clear();								
			}
		}		
	}
	
	/**
	 * Tests whether records can be expired correctly.
	 */
	@Test
	public void testExpirationOfEntries()
	throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				
				// flags to test records as deleted: false (default) is alive, true is deleted
				boolean[] deleted = new boolean[list.size()];
				
				// delete random tuples
				for (int i = 0; i < list.size(); i++) {
					if (random.nextBoolean()) {
						deleted[i] = true;
						page.deleteTuple(i);
					}
				}
				
				// recreate the page through the factory method for existing pages
				byte[] binPage = page.getBuffer();
				page = AbstractExtensionFactory.getExtensionFactory().createTablePage(schema, binPage);
				assertFalse("Page must not have been modified, yet", page.hasBeenModified());
				assertTrue("Record count is wrong.", list.size() == page.getNumRecordsOnPage());
				
				// iterate over the page and check every second tuple
				TupleIterator iter = page.getIterator(numCols, Long.MAX_VALUE);
				for (int i = 0; i < list.size(); i++) {
					if (deleted[i]) {
						// check that no tuple was returned
						assertTrue("Null should have been returned because the tuple was deleted.", page.getDataTuple(i, Long.MAX_VALUE, numCols) == null);
					} else {
						// check that this tuple can be obtained through the iterator
						assertTrue("The iterator's hasNext() method should be true as this tuple was not deleted.", iter.hasNext());
						assertTrue("The iterator's next() method should return a tuple as hasNext() returned true.", list.get(i).equals(iter.next()));
					}
				}
				
				deleted = null; // cleanup, help GC
				// help the garbage collector
				list.clear();				
			}
		}		
	}
	
	/**
	 * Tests whether exceptions are thrown.
	 */
	@Test
	public void testExceptions()
	throws Exception
	{

		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				// check that an expired page throws an exception
				page.markExpired();
				
				try 
				{
					page.getPageNumber();
					fail("No exception was thrown by a call on an expired page.");
				} catch(PageExpiredException e)
				{
					// exception was correctly thrown
				}
				catch(Exception e)
				{
					fail("A wrong kind of exception was thrown.");
				}
			
				try 
				{
					page.getIterator(numCols, Long.MAX_VALUE);
					fail("No exception was thrown by a call on an expired page.");
				} catch(PageExpiredException e)
				{
					// exception was correctly thrown
				}
				catch(Exception e)
				{
					fail("A wrong kind of exception was thrown.");
				}						
				// help the garbage collector
				list.clear();				
			}
		}				

	}
	

	/**
	 * Tests whether the iterators work correctly.
	 */
	@Test
	public void testIterators()
	throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();

				// reverse the list
				Collections.reverse(list);
				
				// init a new page into the old buffer
				byte[] binPage = page.getBuffer();
				page = AbstractExtensionFactory.getExtensionFactory().initTablePage(schema, binPage, 1);
				assertTrue("Page numbers must match.", page.getPageNumber() == 1);
				assertTrue("A page must initially have zero tuples.", page.getNumRecordsOnPage() == 0);
				assertTrue("Modified flag of new pages should be set after initialization", page.hasBeenModified());
				
				// fill reverse with the tuples
				for (int i = 0; i < list.size(); i++) {
					assertTrue(page.insertTuple(list.get(i)));
				}
				
				TupleIterator noRID = page.getIterator(numCols, Long.MAX_VALUE);
				TupleRIDIterator withRID = page.getIteratorWithRID();
				while (withRID.hasNext()) {
					RID rid = withRID.next().getSecond();
					DataTuple t = noRID.next();
					
					assertTrue(t.equals(list.get(rid.getTupleIndex())));
				}
				
				// help the garbage collector
				list.clear();				
			}
		}		
	}
	
	/**
	 * Tests whether the iterator works correctly if it is given some predicates.
	 * @throws Exception
	 */
	@Test
	public void testIteratorWithPredicates() 
		throws Exception
	{
		for(int psi=0; psi < pageSizes.length; psi++)
		{
			for (int sni = 0; sni < NUM_SCHEMAS; sni++)
			{
				Pair<TablePage,List<DataTuple>> listPair = initializeTableList(psi, sni);
				TablePage page = listPair.getFirst();
				List<DataTuple> list = listPair.getSecond();
				TableSchema schema = schemas[psi][sni];
				int numCols = schema.getNumberOfColumns();
	
				// reverse the list
				Collections.reverse(list);
				
				// init a new page into the old buffer
				byte[] binPage = page.getBuffer();
				page = AbstractExtensionFactory.getExtensionFactory().initTablePage(schema, binPage, 1);
				assertTrue("Page numbers must match.", page.getPageNumber() == 1);
				assertTrue("A page must initially have zero tuples.", page.getNumRecordsOnPage() == 0);
				assertTrue("Modified flag of new pages should be set after initialization", page.hasBeenModified());
				
				// fill reverse with the tuples
				for (int i = 0; i < list.size(); i++) {
					assertTrue(page.insertTuple(list.get(i)));
				}
				// check which tuples pass the predicate
				List<DataTuple> passed = new ArrayList<DataTuple>();				
				// fetch and check against the list (all columns of record with predicates)
				LowLevelPredicate[] predicates = generatePredicates(schema, numCols); 
				for (int i = 0; i < list.size(); i++) 
				{
					boolean origPasses = true;
					DataTuple orig = list.get(i);
					for(int x = 0; x < predicates.length; x++)				{
						if (!predicates[x].evaluateWithNull(orig.getField(predicates[x].getColumnIndex())))
						{
							origPasses = false;
						}
					}
					if(origPasses)	
					{
						passed.add(orig);
					}
				}						
				
				TupleIterator noRID = page.getIterator(predicates, numCols, Long.MAX_VALUE);
				while(noRID.hasNext())
				{
					assertTrue("All elements that the iterator return should pass the predicates.", passed.remove(noRID.next()));
				}
	
				assertTrue("The iterator should return all tuples of the page that pass the predicates.", passed.isEmpty());				
			}
		}
	}
	
	
	/**
	 * Generates low level predicates for the columns of the given schema.
	 * 
	 * @param schema The schema of the record.
	 * @param numCols The number of columns in the record.
	 * @return Array of predicates for the schema.
	 * @throws DataFormatException Thrown, if a field could not be generated.
	 */
	protected LowLevelPredicate[] generatePredicates(TableSchema schema,
			int numCols)  throws DataFormatException
	{
		LinkedList<LowLevelPredicate> predicates = new LinkedList<LowLevelPredicate>();
		TreeSet<Integer> columnIndexes = new TreeSet<Integer>();
		for (int i = 0; i < Math.floor(numCols/4); i++)
		{
			columnIndexes.add(random.nextInt(numCols));
		
		}
		for(Integer columnIndex : columnIndexes)
		{
			LowLevelPredicate pred = new LowLevelPredicate(Operator.values()[random.nextInt(Operator.values().length-1)+1], generateRandomField(schema.getColumn(columnIndex).getDataType()), columnIndex);
			predicates.add(pred);
		}
		
		return predicates.toArray(new LowLevelPredicate[predicates.size()]);
	}

	/**
	 * Builds a column bitmap for the given number of columns and selected columns to fetch.
	 * 
	 * @param numCols The number of columns in the tuple.
	 * @param columnsToFetch The columns to be fetched of the tuple.
	 * @return A bitmap describing the columns to be fetched.
	 */
	protected long buildColumnBitmap(int numCols,
			Set<Integer> columnsToFetchOrig) 
	{
		// get the columns and sort them in reverse order
		TreeSet<Integer> columnsToFetch = new TreeSet<Integer>(Collections.reverseOrder());
		columnsToFetch.addAll(columnsToFetchOrig);
		// create bitmap
		long columnBitmap = 0x00000000;
		for (int i = numCols - 1; i >= 0; i--)
		{
			if (columnsToFetch.contains(i))
			{
				columnBitmap = (columnBitmap | 0x1);
			}
			if (i > 0)
			{
				columnBitmap <<= 1;
			}
		}
		return columnBitmap;
	}
	
	/**
	 * Generates a random data type for the schema.
	 * 
	 * @return A random data type.
	 */
	private static DataType getRandomDataType()
	{
		int num = random.nextInt(10);
		
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
			return DataType.charType(random.nextInt(256) + 1);
		case 6:
			return DataType.varcharType(random.nextInt(256) + 1);
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
	 * Generates a data field of the given type with a random value.
	 * 
	 * @param type The type of the data field.
	 * @return A data field with a random value.
	 * @throws DataFormatException Thrown, if the generation of a field fails.
	 */
	private static DataField generateRandomField(DataType type)
	throws DataFormatException
	{
		if (random.nextInt(20) == 0) {
			return type.getNullValue();
		}
		
		switch (type.getBasicType()) {
		case SMALL_INT:
			return new SmallIntField((short) random.nextInt());
		case INT:
			return new IntField(random.nextInt());
		case BIG_INT:
			return new BigIntField(random.nextLong());
		case FLOAT:
			return new FloatField(random.nextFloat());
		case DOUBLE:
			return new DoubleField(random.nextDouble());
		case CHAR:
			return new CharField(getRandomString(type.getLength()));
		case VAR_CHAR:
			return new VarcharField(getRandomString(type.getLength()).substring(0, random.nextInt(type.getLength())));
		case TIME:
			int h = random.nextInt(24);
			int m = random.nextInt(60);
			int s = random.nextInt(60);
			int o = random.nextInt(24*60*60*1000) - 12*60*60*1000;
			return new TimeField(h, m, s, o);
		case TIMESTAMP:
			int yy = random.nextInt(2999) + 1600;
			int tt = random.nextInt(12);
			int dd = random.nextInt(28) + 1;
			int hh = random.nextInt(24);
			int mm = random.nextInt(60);
			int ss = random.nextInt(60);
			int ll = random.nextInt(1000);
			return new TimestampField(dd, tt, yy, hh, mm, ss, ll);
		case DATE:
			int y = random.nextInt(9999) + 1;
			int t = random.nextInt(12);
			int d = random.nextInt(28) + 1;
			return new DateField(d, t, y);
		default:
			return new IntField(1);
		}
	}
	
	/**
	 * Creates a random string of the given length.
	 * 
	 * @param len The length of the string.
	 * @return A random string of the given length.
	 */
	private static String getRandomString(int len)
	{
		StringBuilder buffer = new StringBuilder(len);
		
		for (int i = 0; i < len; i++) {
			int ch = random.nextInt(255) + 1;
			buffer.append((char) ch);
		}
		return buffer.toString();
	}

	/**
	 * Generates a table page and inserts tuples while maintaining a list with inserted tuples.
	 * 
	 * @param pageSizeNr The index of the pagesize in the pageSizes array.
	 * @param schemaNr The index of the schema in the schemas array.
	 * @return A pair of the page and the list with the inserted records.
	 * @throws Exception
	 */
	private static Pair<TablePage,List<DataTuple>> initializeTableList(int pageSizeNr, int schemaNr)
	throws Exception
	{
		int totalMaxWidth = totalMaxWidths[pageSizeNr][schemaNr]; 
		TableSchema schema = schemas[pageSizeNr][schemaNr];
		int pageSize = pageSizes[pageSizeNr];
		// create the binary buffer
		byte[] binPage = new byte[pageSize];
		
		// create a blank page first
		int pageNum = random.nextInt(Integer.MAX_VALUE);
		TablePage page = AbstractExtensionFactory.getExtensionFactory().initTablePage(schema, binPage, pageNum);
			
		assertTrue("Page numbers must match.", page.getPageNumber() == pageNum);
		assertTrue("A page must initially have zero tuples.", page.getNumRecordsOnPage() == 0);
		assertTrue("Modified flag of new pages should be set after initialization", page.hasBeenModified());
		
		// cache to keep the tuples
		List<DataTuple> list = new ArrayList<DataTuple>(256);
		
		// fill until it does not work any more
		DataTuple tuple = null;
		do {
			tuple = new DataTuple(schema.getNumberOfColumns());
			
			// fill with random data
			for (int col = 0; col < schema.getNumberOfColumns(); col++) {
				ColumnSchema cs = schema.getColumn(col);
				tuple.assignDataField(generateRandomField(cs.getDataType()), col);
			}
			list.add(tuple);
		} while (page.insertTuple(tuple));
		
		// remove the last element that did not fit in
		if (list.size() > 0) {
			list.remove(list.size() - 1);
		}

		// check that the record count is all right
		assertTrue("Record count is wrong.", list.size() == page.getNumRecordsOnPage());
		
		// check that we got at least as many tuples as we would get if every
		// tuple used their variable length fields to the maximum
		assertTrue("Page has too few tuples, too much storage is wasted.", 
				page.getNumRecordsOnPage() >= (pageSize - 32) / totalMaxWidth);
		
		// check that the page is now modified if it contains data (tuples may be bigger than page size
		if(page.getNumRecordsOnPage() > 0)
			assertTrue("Page must have been modified by now.", page.hasBeenModified());
		
		return new Pair<TablePage, List<DataTuple>>(page, list);
	}
	
	/**
	 * Generates schemas for the test cases.
	 * 
	 * @param pageSize The size of the page.
	 * @param pageSizeNr The index of the page size in the pageSizes array.
	 */
	static void generateSchemas(int pageSize, int pageSizeNr, TableSchema[][] schemas, Integer[][] totalMaxWidths)
	{
		
		for (int si = 0; si < NUM_SCHEMAS; si++)
		{
			int totalMaxWidth = 4; // start with 4 bytes metadata
			TableSchema schema = new TableSchema(pageSize);
			
			// generate a random set of columns
			int numCols = random.nextInt(20) + 1;
			
			// create the columns as given
			for (int col = 0; col < numCols; col++) {
				DataType type = getRandomDataType();
				schema.addColumn(ColumnSchema.createColumnSchema("Random Column " + col, type, true));
				
				// track how many bytes one tuple can have maximally
				totalMaxWidth += type.getNumberOfBytes();
				if (type.isArrayType() && !type.isFixLength()) {
					// VARCHAR
					totalMaxWidth += 8; // Pointer
				}
			}
			
			schemas[pageSizeNr][si] = schema;
			totalMaxWidths[pageSizeNr][si] = totalMaxWidth;
		}
	}	
	
}
