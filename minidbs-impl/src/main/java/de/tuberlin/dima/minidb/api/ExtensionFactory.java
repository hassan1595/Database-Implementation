package de.tuberlin.dima.minidb.api;

import de.tuberlin.dima.minidb.Config;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageCache;
import de.tuberlin.dima.minidb.io.cache.PageCacheClass;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.index.BTreeIndex;
import de.tuberlin.dima.minidb.io.manager.BufferPoolManager;
import de.tuberlin.dima.minidb.io.manager.PoolManager;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TablePageClass;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;
import de.tuberlin.dima.minidb.mapred.TableInputFormat;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimatorClass;
import de.tuberlin.dima.minidb.optimizer.generator.PhysicalPlanGenerator;
import de.tuberlin.dima.minidb.optimizer.generator.PhysicalPlanGeneratorClass;
import de.tuberlin.dima.minidb.optimizer.joins.JoinOrderOptimizer;
import de.tuberlin.dima.minidb.optimizer.joins.JoinOrderOptimizerClass;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;
import de.tuberlin.dima.minidb.parser.SQLParser;
import de.tuberlin.dima.minidb.qexec.*;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.qexec.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.qexec.predicate.LocalPredicate;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
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
		return new TablePageClass(schema, binaryPage);
	}

	@Override
	public TablePage initTablePage(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException {
		return new TablePageClass(schema, binaryPage, newPageNumber);
	}

	@Override
	public PageCache createPageCache(PageSize pageSize, int numPages) {
		return new PageCacheClass(pageSize, numPages);
	}

	@Override
	public BufferPoolManager createBufferPoolManager(Config config, Logger logger) {
		return new PoolManager(config, logger);
	}

	@Override
	public BTreeIndex createBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TableScanOperator createTableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {
		return new TableScanOperatorClass(bufferPool, tableManager, resourceId, producedColumnIndexes,
				predicate, prefetchWindowLength);
	}

	@Override
	public IndexScanOperator createIndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		return new IndexScanOperatorClass( index,  startKey,  stopKey,  startKeyIncluded,  stopKeyIncluded);
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
		return new NestedLoopJoinOperatorClass(outerChild, innerChild, joinPredicate, columnMapOuterTuple, columnMapInnerTuple);
	}

	@Override
	public IndexLookupOperator getIndexLookupOperator(BTreeIndex index, DataField equalityLiteral) {
		return new IndexLookupOperatorClass(index, equalityLiteral);
	}

	@Override
	public IndexLookupOperator getIndexScanOperatorForBetweenPredicate(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound,
			boolean upperIncluded) {
		return new IndexLookupOperatorClass(index, lowerBound, lowerIncluded, upperBound, upperIncluded);
	}

	@Override
	public IndexCorrelatedLookupOperator getIndexCorrelatedScanOperator(BTreeIndex index, int correlatedColumnIndex) {
		return new IndexCorrelatedLookupOperatorClass(index, correlatedColumnIndex);
	}

	@Override
	public FetchOperator createFetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {
		return  new FetchOperatorClass(child, bufferPool, tableResourceId, outputColumnMap);
	}

	@Override
	public FilterOperator createFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate) {
		return new FilterOperatorClass(child, predicate);
	}

	@Override
	public FilterCorrelatedOperator createCorrelatedFilterOperator(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
		return new FilterCorrelatedOperatorClass(child, correlatedPredicate);
	}

	@Override
	public SortOperator createSortOperator(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		return new SortOperatorClass(child, queryHeap, columnTypes, estimatedCardinality, sortColumns, columnsAscending);
	}

	@Override
	public GroupByOperator createGroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
			AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
		return new GroupByOperatorClass(child, groupColumnIndices, aggColumnIndices, aggregateFunctions, aggColumnTypes, groupColumnOutputPositions, aggregateColumnOutputPosition);
	}

	@Override
	public MergeJoinOperator createMergeJoinOperator(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
		return new MergeJoinOperatorClass(leftChild, rightChild, leftJoinColumns, rightJoinColumns, columnMapLeftTuple, columnMapRightTuple);
	}

	@Override
	public JoinOrderOptimizer createJoinOrderOptimizer(CardinalityEstimator estimator) {
		return new JoinOrderOptimizerClass(estimator);

	}

	@Override
	public CardinalityEstimator createCardinalityEstimator() {
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CostEstimator createCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead) {
		return  new CostEstimatorClass(readCost, writeCost, randomReadOverhead,randomWriteOverhead );
	}

	@Override
	public PhysicalPlanGenerator createPhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {
		return new PhysicalPlanGeneratorClass(catalogue, cardEstimator, costEstimator);
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
