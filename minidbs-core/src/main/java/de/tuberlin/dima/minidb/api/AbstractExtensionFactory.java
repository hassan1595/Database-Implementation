package de.tuberlin.dima.minidb.api;


import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.mapred.TableInputFormat;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.generator.PhysicalPlanGenerator;
import de.tuberlin.dima.minidb.optimizer.joins.JoinOrderOptimizer;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.qexec.*;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.SelectQueryAnalyzer;
import de.tuberlin.dima.minidb.warm_up.Sort;

import java.util.logging.Logger;


/**
 * A simple abstract factory-like extension point that supplies the actual implementation 
 * of classes being initialized with the class name of the concrete factory.
 *  
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class AbstractExtensionFactory
{
	// ------------------------------------------------------------------------
	//                       Singleton Instance
	// ------------------------------------------------------------------------
	
	/**
	 * The singleton instance of this class' implementation.
	 */
	private static AbstractExtensionFactory singletonInstance;

	
	/**
	 * Gets the actual implementation of this <code>ExtansionFactory</code>.
	 * This method can only be called after <code>init</code> has been called.
	 * 
	 * @return The implementation of the <code>ExtansionFactory</code>.
	 * @throws IllegalStateException If <code>init</code> has not been called on
	 *                               the <code>ExtansionFactory</code>.
	 */
	public static final AbstractExtensionFactory getExtensionFactory() throws IllegalStateException
	{
		if (singletonInstance == null) {
			throw new IllegalStateException("Factory has not been initialized!");
		} else {
			return singletonInstance;
		}
	}
	
	/**
	 * Initializes the extension factory from the default implementation name.
	 * 
	 * @throws ExtensionInitFailedException Thrown, if the default resource could not be located,
	 *                                      or the property was undefined.
	 */
	public static final void initializeDefault() throws ExtensionInitFailedException
	{
		if (singletonInstance == null) {
			singletonInstance = new LayeredExtensionFactory();
		}
	}
	
	// ------------------------------------------------------------------------
	//                           Factory Methods
	// ------------------------------------------------------------------------
	
	/**
	 * Gets an implementation for the <code>SQLParser</code>
	 * 
	 * @param sqlStatement The statement to be parsed by the parser.
	 * @return An SQLParser implementation.
	 */
	abstract public SQLParser getParser(String sqlStatement);
	
	/**
	 * Instantiates the select-query analyzer.
	 * 
	 * @return An instance of a select-query analyzer.
	 */
	abstract public SelectQueryAnalyzer createSelectQueryAnalyzer();
	
	/**
	 * Creates a TablePage that gives access to the tuples stored in binary format
	 * in the given byte array. The schema is described in the given TableSchema.
	 * This class does not really contain a copy of the binary data as objects, but it
	 * actually wraps the data and provides functions to access them.
	 * 
	 * @param schema The schema describing the layout of the binary page.
	 * @param binaryPage The binary data from the table.
	 * @return A TablePage that represents the given binary table data.
	 * @throws PageFormatException If the byte array did not contain a valid page as
	 *                             described by the TableSchema.
	 */
	abstract public TablePage createTablePage(TableSchema schema, byte[] binaryPage)
	throws PageFormatException;
	
	/**
	 * Initializes an empty table page with the given page number 
	 * that will store its data in the given byte buffer. This method makes sure a valid header is
	 * created.
	 * 
	 * @param schema The schema describing the layout of the new page.
	 * @param binaryPage The buffer for the table page.
	 * @param newPageNumber The number for the initialized page.
	 * @return The new empty TablePage.
	 * @throws PageFormatException If the array size does not match the page size
	 *                             for this table schema.
	 */
	abstract public TablePage initTablePage(TableSchema schema, byte[] binaryPage, int newPageNumber)
	throws PageFormatException;
	
	/**
	 * Creates a new PageCache with the given number of entries that caches pages of
	 * the given size. During its lifetime, the cache holds a fix number of pages.
	 * 
	 * After creation, the cache initially holds only empty pages, such that the first
	 * <i>numPages</i> <tt>EvictedCacheEntry</tt> objects contain only the byte[] buffer and
	 * a <tt>CacheableData</tt> which is null and a resourceId that is -1.
	 *  
	 * @param pageSize The size of the pages cached by this page cache.
	 * @param numPages The number of pages that the cache holds.
	 * @return The new page cache.
	 */
	abstract public PageCache createPageCache(PageSize pageSize, int numPages);
	
	/**
	 * Creates a buffer pool manager that serves as the resource gateway for all queries.
	 * The buffer pool internally owns the caches for the respective page sizes and
	 * employs a read and a write thread for its I/O operations.
	 * <p>
	 * The buffer pool manager takes all of its configuration values from the given <tt>Config</tt>
	 * object. Such configuration values are the sizes of the caches, or the number of I/O buffers
	 * (the byte arrays initially in the empty buffer queue).
	 * <p>
	 * Note that the buffer pool manager does not accept any directory containing the files that
	 * its I/O operations target. Such files are registered through a 
	 * {@link de.tuberlin.dima.minidb.io.manager.ResourceManager} in the
	 * {@link de.tuberlin.dima.minidb.io.manager.BufferPoolManager#registerResource(int, de.tuberlin.dima.minidb.io.manager.ResourceManager)}
	 * method.
	 * 
	 * @param config The configuration defining the sizes of the caches and the number of I/O buffers.
	 * @param logger The logger that can be used for 
	 * @return An instance of the buffer pool manager, with not yet running I/O threads.
	 */
	abstract public BufferPoolManager createBufferPoolManager(Config config, Logger logger);
	
	/**
	 * Creates a B-Tree Index that allows to evaluate index requests. The index that is evaluated
	 * is given by the resource-Id. All requests for pages go against the given buffer pool manager.
	 * 
	 * @param schema The schema of the index.
	 * @param bufferPool The buffer pool manager that is used to request pages.
	 * @param resourceId The id of the index, which allows to identify the resource.
	 * @return A B-Tree index for the schema using the given buffer pool manager.
	 */
	abstract public BTreeIndex createBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId);
	
	/**
	 * Creates a new physical query plan operator performing a table scan.
	 * 
	 * The table scan operator fetches its pages from the given buffer pool.
	 * The resource containing the pages is specified in the given resource Id.
	 * 
	 * Through the TableResourceManager, the operator can determine the page
	 * numbers of the pages relevant for the scan:
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getFirstDataPageNumber()}
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getLastDataPageNumber()}
	 * 
	 * The array of producedColumnIndexes represents the map from the raw tuple that
	 * can be obtained from the page to the tuple that is produced by the TableScan.
	 * If the array contains {4, 1, 6} then the produced tuple contains only three columns,
	 * namely the ones that are in the tables original columns 4, 1 and 6.
	 * 
	 * The predicate is evaluated on all tuples from the table and only the qualifying
	 * tuples are produced by this operator. The predicate is evaluated on the raw tuple
	 * from the table, not on the produced tuple.
	 * 
	 * @param bufferPool The buffer pool used to get the pages that are scanned.
	 * @param tableManager The table manager used to get the page numbers of the pages
	 *                     relevant to the scan.
	 * @param resourceId The resource id of the table resource.
	 * @param producedColumnIndexes The indexes of the columns that occur in the produced
	 *                              tuple in the order as they are produced.
	 * @param predicate An array of predicates each tuple must pass. The predicates are conjunctively
	 *              connected, so if any of the predicates evaluates to false, the tuple is discarded.
	 * @param prefetchWindowLength The number of pages to prefetch in advance to the page
	 *                             that the operator currently works on.
	 * @return A new physical plan operator representing a TableScan.
	 */
	abstract public TableScanOperator createTableScanOperator(
			BufferPoolManager bufferPool,
			TableResourceManager tableManager,
			int resourceId,
			int[] producedColumnIndexes,
			LowLevelPredicate[] predicate,
			int prefetchWindowLength
			);

	/**
	 * Creates a new physical query plan operator performing an index scan.
	 *
	 * The index scan retrieves the keys from the given index.
	 * 
	 * The provided keys (startKey and stopKey) along with the inclusion flags describe
	 * the range of keys to retrieve using the index scan operator.
	 *
	 * @param index The B+-Tree that is scanned.
	 * @param startKey The lower boundary of the requested interval.
	 * @param stopKey The upper boundary of the requested interval.
	 * @param startKeyIncluded A flag indicating whether the lower boundary is inclusive. True indicates an inclusive boundary. 
	 * @param stopKeyIncluded A flag indicating whether the upper boundary is inclusive. True indicates an inclusive boundary.
	 * @return A new physical plan operator representing an IndexScan.
	 */
	abstract public IndexScanOperator createIndexScanOperator(
			BTreeIndex index,
			DataField startKey, 
			DataField stopKey, 
			boolean startKeyIncluded, 
			boolean stopKeyIncluded
			);

	/**
	 * Creates a new physical query plan operator performing an insert.
	 * 
	 * The insert operator fetches its pages from the given buffer pool.
	 * The resource containing the pages is specifi	abstract public TableScanOperator createTableScanOperator(
			BufferPoolManager bufferPool,
			TableResourceManager tableManager,
			int resourceId,
			int[] producedColumnIndexes,
			LowLevelPredicate[] predicate,
			int prefetchWindowLength
			);ed in the given resource Id.
	 * 
	 * Through the TableResourceManager, the operator can determine the page
	 * numbers of the pages relevant for the deletion:
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getFirstDataPageNumber()}
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getLastDataPageNumber()}
	 * 
	 * The insert operator needs to update the provided indexes for the different columns using the 
	 * given column numbers.
	 * 
	 * @param bufferPool The buffer pool used to get the pages that are scanned.
	 * @param tableManager The table manager used to get the page numbers of the pages
	 *                     relevant to the scan.
	 * @param resourceId The resource id of the table resource.
	 * @param indexes The indexes of the table that need to be updated with the inserted values.
	 * @param columnNumbers The column numbers that the different indexes refer to.
	 * @param child The child operator generating the tuples to be inserted.
	 * @return A new physical plan operator representing an InsertOperator.
	 */
	abstract public InsertOperator createInsertOperator(
			BufferPoolManager bufferPool,
			TableResourceManager tableManager,
			int resourceId,
			BTreeIndex[] indexes,
			int[] columnNumbers,
			PhysicalPlanOperator child
			);

	/**
	 * Creates a new physical query plan operator performing a delete.
	 * 
	 * The delete operator fetches its pages from the given buffer pool.
	 * The resource containing the pages is specified in the given resource Id.
	 * 
	 * @param bufferPool The buffer pool used to get the pages that are scanned.
	 * @param resourceId The resource id of the table resource.
	 * @param child The child operator generating the tuples to be deleted.
	 * @return A new physical plan operator representing a DeleteOperator.
	 */
	abstract public DeleteOperator createDeleteOperator(
			BufferPoolManager bufferPool,
			int resourceId,
			PhysicalPlanOperator child
			);

	/**
	 * Creates a new Nested-Loop-Join operator, drawing tuples from the outer side in the outer
	 * loop and from the inner side in the inner loop. The inner side is opened and closed for
	 * each tuple from the outer side. The join optionally evaluates a join predicate. Optionally
	 * is because in some cases, no join predicate exists (Cartesian Join), or the join predicate
	 * is already represented in the correlation.
	 *  
	 * The tuples produced by the join operator are (in most cases) a concatenation of
	 * the tuple from outer and inner side. How the columns from the output tuple are derived
	 * from the columns of the input tuple is described in two map arrays:
	 * <tt>columnMapOuterTuple</tt> and <tt>columnMapInnerTuple</tt>. At position <tt>i</tt> in
	 * such a map array is the position of the column in the outer (respectively inner) tuple that
	 * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
	 * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
	 * outer tuple (respectively inner) tuple.
	 * 
	 * Here is an example of how to assign the fields of the outer tuple to the output tuple:
	 * <code>
	 * for (int i = 0; i < outerTupleColumnMap.length; i++) {
	 *     int index = outerTupleColumnMap[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(currentOuterTuple.getField(index), i);
	 *     }
	 * }
	 * </code>
	 *  
	 * @param outerChild The outer child for the nested-loop-join, whose tuple are pulled in the
	 *                   outer loop.
	 * @param innerChild The inner child for the nested-loop-join, whose tuples are pulled in the
	 *                   inner loop.
	 * @param joinPredicate The join predicate to be evaluated on the tuples. The predicate is
	 *                      constructed such that the tuples from the outer child are the left hand
	 *                      side argument and the tuples from the inner child are the right hand side
	 *                      argument.
	 * @param columnMapOuterTuple The map describing how the columns from the outer tuple are copied
	 *                            to the output tuple. See also above column.
	 * @param columnMapInnerTuple The map describing how the columns from the inner tuple are copied
	 *                            to the output tuple. See also above column.
	 * @return An implementation of the NestedLoopJoinOperator.
	 */
	abstract public NestedLoopJoinOperator createNestedLoopJoinOperator(
			PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild,
            JoinPredicate joinPredicate,
            int[] columnMapOuterTuple, int[] columnMapInnerTuple);

	/**
	 * Creates an index lookup operator that returns the RIDs for the key
	 * given as the equality literal. This index scan is used to evaluate a local equality
	 * predicate like <code> t1.someColumn = "Value" </code>.
	 * 
	 * @param index The index object used to access the index.
	 * @param equalityLiteral The key that the index returns the RIDs for.
	 * @return An implementation of the IndexScanOperator.
	 */
	abstract public IndexLookupOperator getIndexLookupOperator(BTreeIndex index, DataField equalityLiteral);

	/**
	 * Creates an index lookup operator returning the RIDs for the tuples in the given range.
	 * This index scan is normally used to evaluate a between predicate:
	 * <code> "val1" &lt;(=) key &lt;(=) "val2" </code>.
	 * Note that any range predicate with only one bound can be converted to such a "between predicate" by
	 * choosing the other bound as the minimal or maximal value in the domain.
	 * 
	 * @param index The index object used to access the index.
	 * @param lowerBound The lower bound of the range.
	 * @param lowerIncluded Flag indicating whether the lower bound itself is included in the range.
	 * @param upperBound The upper bound of the range.
	 * @param upperIncluded Flag indicating whether the upper bound itself is included in the range.
	 * @return An implementation of the IndexScanOperator.
	 */
	abstract public IndexLookupOperator getIndexScanOperatorForBetweenPredicate(BTreeIndex index,
	                                                                  DataField lowerBound, boolean lowerIncluded,
                                                                      DataField upperBound, boolean upperIncluded);

	/**
	 * Creates an index lookup operator that works in a correlated fashion. For each time it is opened, 
	 * it returns the RIDs for the key equal to the correlated tuple's column at the specified position. 
	 * 
	 * @param index The index object used to access the index.
	 * @param correlatedColumnIndex The index of the column in the correlated tuple that we evaluate against.
	 * @return An implementation of the IndexCorrelatedScanOperator.
	 */
	abstract public IndexCorrelatedLookupOperator getIndexCorrelatedScanOperator(BTreeIndex index, int correlatedColumnIndex);

	/**
	 * Creates a new FETCH operator that takes RIDs to get tuples from a table.
	 * 
	 * The FETCH operator implicitly performs a projection (no duplicate elimination)
	 * by copying only some fields from the fetched tuple to the tuple that is produced.
	 * This is described in the column-map-array. Position <tt>i</tt> in
	 * that map array holds the position of the column in the fetched tuple that
	 * goes to position <tt>i</tt> of the output tuple.
	 * If the array contains {4, 1, 6} then the produced tuple contains only three columns,
	 * namely the ones that are in the tables original columns 4, 1 and 6.
	 * 
	 * @param child The child operator of this fetch operator.
	 * @param bufferPool The buffer pool used to take the pages from.
	 * @param tableResourceId The resource id of the table that the tuples are fetched from.
	 * @param outputColumnMap The map describing how the column of the tuple produced by the
	 *                        FETCH operator are produced from the tuple fetched from the table.
	 * @return An implementation of the FetchOperator.
	 */
	abstract public FetchOperator createFetchOperator(PhysicalPlanOperator child,
			BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap);
	
	/**
	 * Creates a new filter operator that evaluates a local predicate on the incoming tuples.
	 * The filter does not work correlated, it applies no predicate against a correlated
	 * tuple.
	 * 
	 * @param child The child of this operator.
	 * @param predicate The predicate to be evaluated on the incoming tuples.
	 * @return An implementation of the FilterOperator.
	 */
	abstract public FilterOperator createFilterOperator(PhysicalPlanOperator child,
	                                                  LocalPredicate predicate);

	
	/**
	 * Creates a new filter operator that evaluates a local predicate 
	 * and also a predicate against a correlated tuple. The tuple stream below the
	 * filter will hence be uncorrelated, the tuple stream above the filter will be
	 * correlated.
	 * 
	 * @param child The child of this operator.
	 * @param correlatedPredicate The correlated predicate. The predicate is constructed such that
	 *                            the current tuple will be the left hand argument and the correlated tuple
	 *                            the right hand argument.
	 * @return An implementation of the FilterOperator.
	 */
	abstract public FilterCorrelatedOperator createCorrelatedFilterOperator(PhysicalPlanOperator child,
                                                                   JoinPredicate correlatedPredicate);
	
	/**
	 * Creates a new sort operator that performs an external merge-sort.
	 * 
	 * @param child The child of the operator, producing the tuples to be sorted.
	 * @param queryHeap The heap which manages the memory and manages reading and writing of the
	 *                  temporary lists.
	 * @param columnTypes An array of the types of the tuple's fields, describing the data type
	 *                    and its length. Used to estimate the memory consumption of sets of tuples
	 *                    and to make the tuples serializable/deserializable for external sorting.
	 * @param estimatedCardinality The estimated number of tuples to sort. Used to determine the amount
	 *                             of space to allocate.
	 * @param sortColumns The indices of the columns after which to sort. The primary sort column
	 *                    is <tt>sortColumns[0]</tt>, the secondary sort column is
	 *                    <tt>sortColumns[1]</tt>, and so on...
	 * @param columnsAscending An array indicating the sort direction of a column. For example, if
	 *                         <code>columnsAscending[1] == true</code>, then the secondary sort
	 *                         column is to be sorted in ascending order. If it is false, then it is
	 *                         to be sorted in descending order.  
	 * @return An implementation of the SortOperator.
	 */
	abstract public SortOperator createSortOperator(PhysicalPlanOperator child,
			QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending);
	
	/**
	 * Creates a group by operator that groups and aggregates a sorted stream of tuples.
	 * 
	 * The arrays <tt>groupColumnIndices</tt> and <tt>aggColumnIndices</tt> describe which
	 * fields in the incoming tuple are used as grouping columns and which ones are used
	 * as aggregate columns.
	 * Note: the input tuple may have more columns than the grouping and aggregate columns.
	 *       Those are simply ignored and dropped.
	 * <p>      
	 * The <tt>aggregateFunctions</tt> array describes which aggregate function is to be used
	 * on the aggregate columns. Note that the functions COUNT, MIN and MAX work on all data
	 * types. The functions SUM and AVG work only on data types implementing the interface
	 * <tt>ArithmeticType</tt>. The implementation of these aggregation functions should cast the
	 * field to that type and use the provided functions to perform calculations.
	 * <p>
	 * The produced tuples are a concatenation of grouping columns and aggregated columns
	 * in a certain order. Here is an example demonstrating how the indices arrays describe
	 * that. Both arrays must have the same length, which is the number of columns in the
	 * output tuple.
	 * <code>
	 * for (int i = 0; i < groupColumnOutputPositions.length; i++) {
	 *     int index = groupColumnOutputPositions[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(currentGroup[index], i);
	 *     }
	 * }
	 * 	for (int i = 0; i < aggregateColumnOutputPosition.length; i++) {
	 *     int index = aggregateColumnOutputPosition[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(aggregatedField[index], i);
	 *     }
	 * }
	 * </code>
	 * 
	 * @param child The child of the operator, delivering the tuples to be grouped and aggregated
	 *              in a sorted manner.
	 * @param groupColumnIndices The indices of the grouping columns in the input tuple.
	 * @param aggColumnIndices The indices of the aggregate columns in the input tuple.
	 * @param aggregateFunctions The functions that are used for aggregation.
	 * @param aggColumnTypes The types of the aggregated functions.
	 * @param groupColumnOutputPositions The map describing in which position in the produced tuple
	 *                                   the grouping columns will be put.
	 * @param aggregateColumnOutputPosition The map describing in which position in the produced tuple
	 *                                   the aggregate columns will be put.
	 * @return An implementation of the GroupByOperator.
	 */
	abstract public GroupByOperator createGroupByOperator(PhysicalPlanOperator child,
			int[] groupColumnIndices, int[] aggColumnIndices,
			OutputColumn.AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes,
			int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition);

	/**
	 * Creates a merge join operator that joins two sorted streams of tuples.
	 * <p>
	 * The tuples of the left and right input streams are joined on the columns described by the
	 * <tt>leftJoinColumns</tt> and <tt>rightJoinColumns</tt> in such a way that the value of the 
	 * column at index <tt>leftJoinColumns[i]</tt> should be equal to the value of the column at
	 * index <tt>rightJoinColumns[i]</tt>. 
	 * <p>
	 * The tuples produced by the join operator are (in most cases) a concatenation of
	 * the tuple from the left and right side. How the columns from the output tuple are derived
	 * from the columns of the input tuples is described in two map arrays:
	 * <tt>columnMapLeftTuple</tt> and <tt>columnMapRightTuple</tt>. At position <tt>i</tt> in
	 * such a map array is the position of the column in the left (respectively right) tuple that
	 * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
	 * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
	 * left tuple (respectively right) tuple.
	 * <p>
	 * Here is an example of how to assign the fields of the left tuple to the output tuple:
	 * <code>
	 * for (int i = 0; i < leftTupleColumnMap.length; i++) {
	 *     int index = leftTupleColumnMap[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(currentLeftTuple.getField(index), i);
	 *     }
	 * }
	 * </code>
	 * nt[] aggregateColumnOutputPosition) {
		throw new UnsupportedOperationException("Method not yet supported");
	 * @param leftChild The left operator producing tuples to be joined.
	 * @param rightChild The right operator producing tuples to be joined.
	 * @param leftJoinColumns The columns of the left tuple to be joined.
	 * @param rightJoinColumns The columns of the right tuple to be joined.
	 * @param columnMapLeftTuple The map describing how the columns from the outer tuple are copied
	 *                            to the output tuple. See also above column.
	 * @param columnMapRightTuple The map describing how the columns from the inner tuple are copied
	 *                            to the output tuple. See also above column.
	 * @return An implementation of the MergeJoinOperator.
	 */
	abstract public MergeJoinOperator createMergeJoinOperator(PhysicalPlanOperator leftChild, 
			PhysicalPlanOperator rightChild, int[] leftJoinColumns, 
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple);
	
	/**
	 * Creates an optimizer for the order of joins.
	 * 
	 * @param estimator The cardinality estimator to be used to cost the plans.
	 * @return The optimizer picking the optimal order of joins.
	 */
	abstract public JoinOrderOptimizer createJoinOrderOptimizer(CardinalityEstimator estimator);
	
	/**
	 * Crates a cardinality estimator for logical operators.
	 * 
	 * @return CardinalityEstimator
	 */
	abstract public CardinalityEstimator createCardinalityEstimator();
	
	/**
	 * Creates a physical operator cost estimator.
	 * 
	 * @param readCost The default time (nanoseconds) that is needed to transfer a block of the
	 *                  default block size from secondary storage to main memory.
	 * @param writeCost The default time (nanoseconds) that is needed to transfer a block of the
	 *                  default block size from main memory to secondary storage.
	 * @param randomReadOverhead The overhead for a single block read operation if the block is
	 *                           not part of a sequence that is read. For magnetic disks, that 
	 *                           would correspond to seek time + rotational latency.
	 * @param randomWriteOverhead The overhead for a single block write if the block is not part 
	 *                            of a sequence that is written. For magnetic disks, that would 
	 *                            correspond to seek time + rotational latency.
	 * @return The generator responsible for physical plan generation.
	 */
	abstract public CostEstimator createCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead);

	/**
	 * Creates a physical plan generator.
	 * 
	 * @param catalogue The catalogue used to determine the availability of indexes.
	 * @param cardEstimator The cardinality estimator (for logical operators).
	 * @param costEstimator The cost estimator (for physical operators).
	 */
	abstract public PhysicalPlanGenerator createPhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator);

	/**
	 * Creates an instance of the TableInputFormat that is used to pass tables into Hadoop.
	 */
	abstract public Class<? extends TableInputFormat> getTableInputFormat();
	
	/**
	 * Creates an instance of a Hadoop table scan operator.
	 * 
	 * @param instance
	 * @param child Operator that produces the input for this operator.
	 * @param predicate	Predicate that will be applied to the input.
	 */
	abstract public HadoopOperator<?, ?> createHadoopTableScanOperator(
			DBInstance instance,
			BulkProcessingOperator child,
			LocalPredicate predicate);
	
	/**
	 * Creates an instance of a Hadoop Group-By operator.
	 * 
	 * For the parameter explanation see {@link de.tuberlin.dima.minidb.api.AbstractExtensionFactory#createGroupByOperator}.
	 */
	abstract public HadoopOperator<?, ?> createHadoopGroupByOperator(
			DBInstance instance,
			BulkProcessingOperator child,
			int[] groupColumnIndices, int[] aggColumnIndices,
			OutputColumn.AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes, int[] groupColumnOutputPositions, 
			int[] aggregateColumnOutputPosition);

	/**
	 * Creates an instance of Sort operator for warm-up task
	 */
	abstract public Sort createSortOperator();
}
