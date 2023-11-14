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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public final class LayeredExtensionFactory extends AbstractExtensionFactory {

    private List<AbstractExtensionFactory> factories = new ArrayList<AbstractExtensionFactory>();

    public LayeredExtensionFactory() throws ExtensionInitFailedException {
        this.registerFactory("de.tuberlin.dima.minidb.api.ReferenceExtensionFactory", false); // optional
        this.registerFactory("de.tuberlin.dima.minidb.api.ExtensionFactory", true); // required
    }

	/**
	 * Initializes this <code>ExtensionFactory</code> with the class name of the
	 * actual implementation.
	 *
	 * @param implementationClass
	 *        The name of the class that holds the actual
	 *        implementation.
	 * @param required
	 *        Whether the factory is required. If not, a <code>ClassNotFoundException</code> is ignored.
	 * @throws ExtensionInitFailedException
	 *         If the class could for some reason not be
	 *         initialized. The Exception contains further information.
	 */
	private void registerFactory(String implementationClass, boolean required) throws ExtensionInitFailedException {
		try {
			Class<? extends AbstractExtensionFactory> ec = Class.forName(implementationClass).asSubclass(AbstractExtensionFactory.class);
			this.factories.add(ec.newInstance());
		} catch (ClassNotFoundException cnfex) {
			if (required) {
				throw new ExtensionInitFailedException("The class '" + implementationClass + "' was not found. Check that it is in the class path.", cnfex);
			}
		} catch (ExceptionInInitializerError eiiex) {
			throw new ExtensionInitFailedException("The class '" + implementationClass + "' could not be loaded, because errors occurred in the initializers.", eiiex);
		} catch (ClassCastException ccex) {
			throw new ExtensionInitFailedException("The class '" + implementationClass + "' is not a subclass of '" + AbstractExtensionFactory.class.getName() + "'.", ccex);
		} catch (Throwable t) {
			throw new ExtensionInitFailedException("The class '" + implementationClass + "' could not be loaded due to an unknown error." + t.getMessage(), t);
		}
	}

	@Override
	public SQLParser getParser(String sqlStatement) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.getParser(sqlStatement);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public SelectQueryAnalyzer createSelectQueryAnalyzer() {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createSelectQueryAnalyzer();
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TablePage createTablePage(TableSchema schema, byte[] binaryPage) throws PageFormatException {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createTablePage(schema, binaryPage);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TablePage initTablePage(TableSchema schema, byte[] binaryPage, int newPageNumber) throws PageFormatException {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.initTablePage(schema, binaryPage, newPageNumber);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public PageCache createPageCache(PageSize pageSize, int numPages) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createPageCache(pageSize, numPages);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public BufferPoolManager createBufferPoolManager(Config config, Logger logger) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createBufferPoolManager(config, logger);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public BTreeIndex createBTreeIndex(IndexSchema schema, BufferPoolManager bufferPool, int resourceId) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createBTreeIndex(schema, bufferPool, resourceId);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public TableScanOperator createTableScanOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId,
			int[] producedColumnIndexes, LowLevelPredicate[] predicate, int prefetchWindowLength) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createTableScanOperator(bufferPool, tableManager, resourceId, producedColumnIndexes, predicate, prefetchWindowLength);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexScanOperator createIndexScanOperator(BTreeIndex index, DataField startKey, DataField stopKey, boolean startKeyIncluded, boolean stopKeyIncluded) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createIndexScanOperator(index, startKey, stopKey, startKeyIncluded, stopKeyIncluded);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public InsertOperator createInsertOperator(BufferPoolManager bufferPool, TableResourceManager tableManager, int resourceId, BTreeIndex[] indexes,
			int[] columnNumbers, PhysicalPlanOperator child) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createInsertOperator(bufferPool, tableManager, resourceId, indexes, columnNumbers, child);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public DeleteOperator createDeleteOperator(BufferPoolManager bufferPool, int resourceId, PhysicalPlanOperator child) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createDeleteOperator(bufferPool, resourceId, child);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public NestedLoopJoinOperator createNestedLoopJoinOperator(PhysicalPlanOperator outerChild, PhysicalPlanOperator innerChild, JoinPredicate joinPredicate,
			int[] columnMapOuterTuple, int[] columnMapInnerTuple) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createNestedLoopJoinOperator(outerChild, innerChild, joinPredicate, columnMapOuterTuple, columnMapInnerTuple);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexLookupOperator getIndexLookupOperator(BTreeIndex index, DataField equalityLiteral) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.getIndexLookupOperator(index, equalityLiteral);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexLookupOperator getIndexScanOperatorForBetweenPredicate(BTreeIndex index, DataField lowerBound, boolean lowerIncluded, DataField upperBound,
			boolean upperIncluded) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.getIndexScanOperatorForBetweenPredicate(index, lowerBound, lowerIncluded, upperBound, upperIncluded);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public IndexCorrelatedLookupOperator getIndexCorrelatedScanOperator(BTreeIndex index, int correlatedColumnIndex) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.getIndexCorrelatedScanOperator(index, correlatedColumnIndex);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FetchOperator createFetchOperator(PhysicalPlanOperator child, BufferPoolManager bufferPool, int tableResourceId, int[] outputColumnMap) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createFetchOperator(child, bufferPool, tableResourceId, outputColumnMap);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FilterOperator createFilterOperator(PhysicalPlanOperator child, LocalPredicate predicate) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createFilterOperator(child, predicate);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public FilterCorrelatedOperator createCorrelatedFilterOperator(PhysicalPlanOperator child, JoinPredicate correlatedPredicate) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createCorrelatedFilterOperator(child, correlatedPredicate);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public SortOperator createSortOperator(PhysicalPlanOperator child, QueryHeap queryHeap, DataType[] columnTypes, int estimatedCardinality,
			int[] sortColumns, boolean[] columnsAscending) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createSortOperator(child, queryHeap, columnTypes, estimatedCardinality, sortColumns, columnsAscending);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public GroupByOperator createGroupByOperator(PhysicalPlanOperator child, int[] groupColumnIndices, int[] aggColumnIndices,
			AggregationType[] aggregateFunctions, DataType[] aggColumnTypes, int[] groupColumnOutputPositions, int[] aggregateColumnOutputPosition) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createGroupByOperator(child, groupColumnIndices, aggColumnIndices, aggregateFunctions, aggColumnTypes,
					groupColumnOutputPositions, aggregateColumnOutputPosition);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public MergeJoinOperator createMergeJoinOperator(PhysicalPlanOperator leftChild, PhysicalPlanOperator rightChild, int[] leftJoinColumns,
			int[] rightJoinColumns, int[] columnMapLeftTuple, int[] columnMapRightTuple) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createMergeJoinOperator(leftChild, rightChild, leftJoinColumns, rightJoinColumns, columnMapLeftTuple, columnMapRightTuple);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public JoinOrderOptimizer createJoinOrderOptimizer(CardinalityEstimator estimator) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createJoinOrderOptimizer(estimator);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CardinalityEstimator createCardinalityEstimator() {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createCardinalityEstimator();
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public CostEstimator createCostEstimator(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createCostEstimator(readCost, writeCost, randomReadOverhead, randomWriteOverhead);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public PhysicalPlanGenerator createPhysicalPlanGenerator(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createPhysicalPlanGenerator(catalogue, cardEstimator, costEstimator);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public Class<? extends TableInputFormat> getTableInputFormat() {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.getTableInputFormat();
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public HadoopOperator<?, ?> createHadoopTableScanOperator(
			DBInstance instance, BulkProcessingOperator child,
			LocalPredicate predicate) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createHadoopTableScanOperator(
						instance, child, predicate);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public HadoopOperator<?,?> createHadoopGroupByOperator(DBInstance instance,
			BulkProcessingOperator child, int[] groupColumnIndices,
			int[] aggColumnIndices, AggregationType[] aggregateFunctions,
			DataType[] aggColumnTypes, int[] groupColumnOutputPositions,
			int[] aggregateColumnOutputPosition) {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createHadoopGroupByOperator(
						instance, child, groupColumnIndices, aggColumnIndices,
						aggregateFunctions, aggColumnTypes,
						groupColumnOutputPositions,
						aggregateColumnOutputPosition);
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}

	@Override
	public Sort createSortOperator() {
		for (AbstractExtensionFactory factory: this.factories) {
			try {
				return factory.createSortOperator();
			} catch (UnsupportedOperationException e) {
				// ignore exception
			}
		}
		throw new UnsupportedOperationException("Method not yet supported");
	}
}
