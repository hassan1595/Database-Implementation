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
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.qexec.*;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.SelectQueryAnalyzer;
import de.tuberlin.dima.minidb.warm_up.Sort;
import de.tuberlin.dima.minidb.warm_up.SortImpl;

import java.util.logging.Logger;

public class ExtensionFactory extends AbstractExtensionFactory {

	@Override
	public SelectQueryAnalyzer createSelectQueryAnalyzer() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TablePage createTablePage(TableSchema schema, byte[] binaryPage) throws PageFormatException {
		return new TablePage(schema, binaryPage);
	}

	@Override
	public TablePage initTablePage(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException {
		return new TablePage(schema, binaryPage, newPageNumber);
	}

	@Override
	public PageCache createPageCache(PageSize pageSize, int numPages) {
		return new PageCache(pageSize, numPages);
	}

	@Override
	public BufferPoolManager createBufferPoolManager(Config config, Logger logger) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public BTreeIndex createBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TableScanOperator createTableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexScanOperator createIndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public InsertOperator createInsertOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId, BTreeIndex[] indexes,
			int[] columnNumbers, PhysicalPlanOperator child) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public DeleteOperator createDeleteOperator(BufferPoolManager bufferPool, int resourceId, PhysicalPlanOperator child) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public NestedLoopJoinOperator createNestedLoopJoinOperator(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate,
			int[] columnMapOuterTuple, int[] columnMapInnerTuple) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexLookupOperator getIndexLookupOperator(BTreeIndex index, DataField equalityLiteral) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexLookupOperator getIndexScanOperatorForBetweenPredicate(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound,
			boolean upperIncluded) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexCorrelatedLookupOperator getIndexCorrelatedScanOperator(BTreeIndex index, int correlatedColumnIndex) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FetchOperator createFetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FilterOperator createFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FilterCorrelatedOperator createCorrelatedFilterOperator(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public SortOperator createSortOperator(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public GroupByOperator createGroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
			AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public MergeJoinOperator createMergeJoinOperator(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public JoinOrderOptimizer createJoinOrderOptimizer(CardinalityEstimator estimator) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CardinalityEstimator createCardinalityEstimator() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CostEstimator createCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public PhysicalPlanGenerator createPhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	/* (non-Javadoc)
	 * @see de.tuberlin.dima.minidb.api.AbstractExtensionFactory#getParser(java.lang.String)
	 */
	@Override
	public SQLParser getParser(String sqlStatement) {
		return null;
	}

	/* Hadoop integration */
	
	@Override
	public Class<? extends TableInputFormat> getTableInputFormat() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public HadoopOperator<?, ?> createHadoopTableScanOperator(
			DBInstance instance, BulkProcessingOperator child,
			LocalPredicate predicate) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public HadoopOperator<?,?> createHadoopGroupByOperator(DBInstance instance,
			BulkProcessingOperator child, int[] groupColumnIndices,
			int[] aggColumnIndices, AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes, int[] groupColumnOutputPositions,
			int[] aggregateColumnOutputPosition) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public Sort createSortOperator() {
		return new SortImpl();
	}
}
