package de.tuberlin.dima.minidb.mapred.qexec.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.ColumnSchema;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;

/**
 * An exemplary operator that counts the tuples in the relation.
 * 
 * This example should demonstrate how to build proper result schematas, and how to
 * use custom Reducers and Combiners.
 * 
 * @author mheimel
 *
 */
public class CountTuplesOperator extends HadoopOperator<NullWritable, IntWritable> {

	/**
	 * Custom Mapper that does nothing but emit one for each element
	 * @author mheimel
	 *
	 */
	public static class CountMapper extends Mapper<Text, DataTuple, 
	                                               NullWritable, IntWritable> {
		@Override
		public void map(Text key, DataTuple value, 
				Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), new IntWritable(1));
		}
	}
	
	/**
	 * Custom combiner that sums up all values.
	 */
	public static class CountCombiner extends Reducer<NullWritable, IntWritable,
	                                                  NullWritable, IntWritable> {
		@Override
		public void reduce(NullWritable key, Iterable<IntWritable> values, 
				Context context) throws IOException, InterruptedException {
			// Sum up all counts.
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(NullWritable.get(), new IntWritable(sum));
		}
	}
	
	/**
	 * Custom reducer that sums up all values and returns the sum of them.
	 * @author mheimel
	 *
	 */
	public static class CountReducer extends Reducer<NullWritable, IntWritable, 
	                                                 NullWritable, DataTuple> {
		@Override
		public void reduce(NullWritable key, Iterable<IntWritable> values, 
				Context context) throws IOException, InterruptedException {
			// Sum up all counts.
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// Build the result data tuple.
			DataTuple tuple = new DataTuple(1);
			tuple.assignDataField(new IntField(sum), 0);
			context.write(NullWritable.get(), tuple);
		}
	}
	
	public CountTuplesOperator(DBInstance instance, 
			BulkProcessingOperator child) throws IOException {
		super(instance, child);
		configureMapCombineReduceJob(
				CountMapper.class, CountCombiner.class, CountReducer.class, 
				NullWritable.class, IntWritable.class);
	}
	
	
	/**
	 * The result is only a single integer value.
	 */
	@Override
	protected TableSchema createResultSchema() {
		TableSchema result_schema = new TableSchema();
		result_schema.addColumn(ColumnSchema.createColumnSchema("count", DataType.intType()));
		return result_schema;
	}


}
