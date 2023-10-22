package de.tuberlin.dima.minidb.qexec;


import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;


/**
 * Factory to instantiate physical query plan operators.
 *  
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class OperatorFactory
{
	/**
	 * The extension point that is used to actually instantiate the classes. 
	 */
	private static AbstractExtensionFactory registry;
	
	
	/**
	 * Creates a new physical query plan operator performing a table scan. The operator
	 * is instantiated through the extension factory.
	 * 
	 * The table scan operator fetches its pages from the given buffer pool.
	 * The resource containing the pages is specified in the given resource Id.
	 * 
	 * Through the TableResourceManager, the operator can determine the page
	 * numbers of the pages relevant for the scan:
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getFirstDataPageNumber()}
	 * {@link de.tuberlin.dima.minidb.io.tables.TableResourceManager#getNextDataPageNumber(int)}
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
	 * The boolean flag for pinning a page indicates that the operator should pin the page
	 * it is currently working on in the cache, and unpin it after it has read all of the
	 * page's tuples. That operation is in some cases relevant for example to table scans
	 * at the outer side of a nested loop join, where the processing of every tuple may
	 * take a fairly long time.
	 * 
	 * @param bufferPool The buffer pool used to get the pages that are scanned.
	 * @param tableManager The table manager used to get the page numbers of the pages
	 *                     relevant to the scan.
	 * @param resourceId The resource id of the table resource.
	 * @param producedColumnIndexes The indexes of the columns that occur in the produced
	 *                              tuple in the order as they are produced.
	 * @param predicate The predicate evaluated within this TableScan.
	 * @param prefetchWindowLength The number of pages to prefetch in advance to the page
	 *                             that the operator currently works on.
	 * @param pinPage True, if the table scan operator should always pin its current page.
	 * @return A new physical plan operator representing a TableScan.
	 */
	public static TableScanOperator createTableScanOperator(
			BufferPoolManager bufferPool,
			TableResourceManager tableManager,
			int resourceId,
			int[] producedColumnIndexes,
			LowLevelPredicate[] predicate,
			int prefetchWindowLength)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createTableScanOperator(bufferPool, tableManager, resourceId, producedColumnIndexes, predicate, prefetchWindowLength);
	}
	
	/**
	 * Creates an index scan operator that returns the RIDs for the key
	 * given as the equality literal. This index scan is used to evaluate a local equality
	 * predicate like <code> t1.someColumn = "Value" </code>.
	 * 
	 * @param index The index object used to access the index.
	 * @param equalityLiteral The key that the index returns the RIDs for.
	 */
	public static IndexLookupOperator createIndexScanOperatorForEqualityPredicate(BTreeIndex index, DataField equalityLiteral)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.getIndexLookupOperator(index, equalityLiteral);
	}


	/**
	 * Creates an index scan operator returning the RIDs for the tuples in the given range.
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
	 */
	public static IndexLookupOperator createIndexScanOperatorForBetweenPredicate(BTreeIndex index,
	                                                                 DataField lowerBound, boolean lowerIncluded,
                                                                     DataField upperBound, boolean upperIncluded)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.getIndexScanOperatorForBetweenPredicate(index, lowerBound, lowerIncluded, upperBound, upperIncluded);
	}

	/**
	 * Creates an index scan operator that works in a correlated fashion. For each time it is opened, 
	 * it returns the RIDs for the key equal to the correlated tuple's column at the specified position. 
	 * 
	 * @param index The index object used to access the index.
	 * @param correlatedColumnIndex The index of the column in the correlated tuple that we evaluate against.
	 */
	public static IndexCorrelatedLookupOperator createIndexCorrelatedLookupOperator(BTreeIndex index, int correlatedColumnIndex)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.getIndexCorrelatedScanOperator(index, correlatedColumnIndex);
	}
	
	
	/**
	 * Creates a new FETCH operator that takes RIDs to get tuples from a table.
	 * 
	 * The FETCH operator implicitly performs a projection (no duplicate elimination)
	 * by copying only some fields from the fetched tuple to the tuple that is produced.
	 * This is described in the column-map-array. Position <tt>i</tt> in
	 * that map array holds the position of the column in the fetched tuple that
	 * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
	 * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
	 * fetched tuple and left blank.
	 * 
	 * Here is an example of how to assign the fields of the outer tuple to the output tuple:
	 * <code>
	 * for (int i = 0; i < outputColumnMap.length; i++) {
	 *     int index = outputColumnMap[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(fetchedTuple.getField(index), i);
	 *     }
	 * }
	 * </code>
	 * 
	 * @param child The child operator of this fetch operator.
	 * @param bufferPool The buffer pool used to take the pages from.
	 * @param tableResourceId The resource id of the table that the tuples are fetched from.
	 * @param outputColumnMap The map describing how the column of the tuple produced by the
	 *                        FETCH operator are produced from the tuple fetched from the table.
	 */
	public static FetchOperator createFetchOperator(PhysicalPlanOperator child,
			BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createFetchOperator(child, bufferPool, tableResourceId, outputColumnMap);
	}
	
	
	/**
	 * Creates a new filter operator that evaluates a local predicate on the incoming tuples.
	 * The filter does not work correlated, it applies no predicate against a correlated
	 * tuple.
	 * 
	 * @param child The child of this operator.
	 * @param predicate The predicate to be evaluated on the incoming tuples.
	 */
	public static FilterOperator createFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createFilterOperator(child, predicate);
	}

	
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
	 */
	public static FilterCorrelatedOperator createdCorrelatedFilterOperator(PhysicalPlanOperator child,
                                                          JoinPredicate correlatedPredicate)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createCorrelatedFilterOperator(child, correlatedPredicate);
	}
	
	
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
	 * inner tuple (respectively outer) tuple.
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
	 * @return A Nested-Loop-Join Operator.
	 */
	public static NestedLoopJoinOperator createNestedLoopJoinOperator(
                                      PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild,
                                      JoinPredicate joinPredicate,
                                      int[] columnMapOuterTuple, int[] columnMapInnerTuple)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createNestedLoopJoinOperator(outerChild, innerChild, joinPredicate, columnMapOuterTuple, columnMapInnerTuple);
	}
	
	/**
	 * Creates a Merge-Join operator that performs an inner equi-join between the two inputs.
	 * The operator expects the input columns to be alreads sorted.
	 * 
	 * The tuples produced by the join operator are (in most cases) a concatenation of
	 * the tuple from outer and inner side. How the columns from the output tuple are derived
	 * from the columns of the input tuple is described in two map arrays:
	 * <tt>leftOutColumnMap</tt> and <tt>rightOutColumnMap</tt>. At position <tt>i</tt> in
	 * such a map array is the position of the column in the left (respectively right) tuple that
	 * goes to position <tt>i</tt> of the output tuple. If position <tt>i</tt> in a map holds
	 * the value <tt>-1</tt>, than that position in the output tuple is not derived from the
	 * right tuple (respectively left) tuple.
	 * 
	 * Here is an example of how to assign the fields of the outer tuple to the output tuple:
	 * <code>
	 * for (int i = 0; i < leftOutColumnMap.length; i++) {
	 *     int index = leftOutColumnMap[i];
	 *     if (index != -1) {
	 *         outputTuple.assignDataField(currentLeftTuple.getField(index), i);
	 *     }
	 * }
	 * </code>
	 * 
	 * @param leftChild The left input to the merge join. 
	 * @param rightChild The right input to the merge join.
	 * @param leftJoinColumns The indices of the join columns in the left input.
	 * @param rightJoinColumns The indices of the join columns in the right input.
	 * @param leftOutColumnMap The map describing which position in the left input tuple goes
	 *                         to which position in the output tuple. 
	 * @param rightOutColumnMap The map describing which position in the right input tuple goes
	 *                          to which position in the output tuple.
	 * @return A Merge-Join-Operator.
	 */
	public static MergeJoinOperator createMergeJoinOperator(
			PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild,
			                     int[] leftJoinColumns, int[] rightJoinColumns,
			                     int[] leftOutColumnMap, int[] rightOutColumnMap)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createMergeJoinOperator(leftChild, rightChild, leftJoinColumns, rightJoinColumns, leftOutColumnMap, rightOutColumnMap);
	}
	
	/**
	 * Creates a new sort operator that performs an external merge-sort.
	 * 
	 * @param child The child of the operator, producing the tuples to be sorted.
	 * @param queryHeap The heap which manages the memory and manages reading and writing of the
	 *                  temporary lists.
	 * @param tupleSchema An array of the data types of the tuple's fields, describing the data type
	 *                    and its length. Used to estimate the memory consumption of sets of tuples.
	 * @param estimatedCardinality The estimated number of tuples to sort.
	 * @param sortColumns The indices of the columns after which to sort. The primary sort column
	 *                    is <tt>sortColumns[0]</tt>, the secondary sort column is
	 *                    <tt>sortColumns[1]</tt>, and so on...
	 * @param columnsAscending An array indicating the sort direction of a column. For example, if
	 *                         <code>columnsAscending[1] == true</code>, then the secondary sort
	 *                         column is to be sorted in ascending order. If it is false, then it is
	 *                         to be sorted in descending order.  
	 * @return An implementation of the SortOperator.
	 */
	public static SortOperator createSortOperator(PhysicalPlanOperator child,
			QueryHeap queryHeap, DataType[] tupleSchema, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createSortOperator(child, queryHeap, tupleSchema,
				estimatedCardinality, sortColumns, columnsAscending);
	}
	
	/**
	 * Create a group by operator that groups and aggregates a sorted stream of tuples.
	 * 
	 * 
	 * The arrays <tt>groupColumnIndices</tt> and <tt>aggColumnIndices</tt> describe which
	 * fields in the incoming tuple are used as grouping columns and which ones are used
	 * as aggregate columns.
	 * Note: the input tuple may have more columns than the grouping and aggregate columns.
	 *       Those are simply ignored and dropped.
	 *       
	 * The <tt>aggregateFunctions</tt> array describes which aggregate function is to be used
	 * on the aggregate columns. Note that the functions COUNT, MIN and MAX work on all data
	 * types. The functions SUM and AVG work only on data types implementing the interface
	 * <tt>ArithmeticType</tt>. The implementation of these aggregation functions should cast the
	 * field to that type and use the provided functions to perform calculations.
	 * 
	 * The produced tuples are a concatenation of grouping columns and aggregated columns
	 * in arbitrary order. Here is an example demonstrating how the indices arrays describe
	 * that. 
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
	 * @param aggregateColumnOutputPosition The map describing in which position in the produced
	 *                                      tuple the aggregate columns will be put.
	 * @return An implementation of the GroupByOperator.
	 */
	public static GroupByOperator createGroupByOperator(PhysicalPlanOperator child,
			int[] groupColumnIndices, int[] aggColumnIndices,
			OutputColumn.AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes,
			int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition)
	{
		if (registry == null) {
			registry = AbstractExtensionFactory.getExtensionFactory();
		}
		
		return registry.createGroupByOperator(child, groupColumnIndices, aggColumnIndices,
				aggregateFunctions, aggColumnTypes, 
				groupColumnOutputPositions, aggregateColumnOutputPosition);
	}
}
