package de.tuberlin.dima.minidb.mapred.qexec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.TableInputFormat;
import de.tuberlin.dima.minidb.mapred.TableOutputFormat;

/**
 * Template class for implementing an operator that uses Hadoop to process
 * table files.
 * 
 * @author mheimel
 *
 */
@SuppressWarnings("rawtypes")
public abstract class HadoopOperator<K extends WritableComparable, 
									 V extends Writable> 
	                  extends BulkProcessingOperator {
	/**
	 * Hadoop job that will be run by this operator.
	 */
	protected Job job;
	
	/**
	 * Configuration for the Hadoop job.
	 */
	protected Configuration job_conf;
	
	/**
	 * Marks whether configure was called on the job.
	 */
	private boolean job_configured = false;
	
	/**
	 * Construct a Hadoop operator that has a single children.
	 * 
	 * @param instance
	 * @param child
	 * @throws IOException 
	 */
	public HadoopOperator(DBInstance instance, 
			BulkProcessingOperator child) throws IOException {
		this(instance, child, null);
	}
	
	/**
	 * Construct a Hadoop operator that has two children (left and right).
	 * 
	 * @param instance
	 * @param left_child
	 * @param right_child
	 * @throws IOException 
	 */
	public HadoopOperator(DBInstance instance, 
			BulkProcessingOperator left_child, 
			BulkProcessingOperator right_child) throws IOException {
		super(instance);
		super.addChild(left_child);
		if (right_child != null) super.addChild(right_child);
		// Create a new job and configure it correspondingly.
		job = new Job();
		job_conf = job.getConfiguration();
		
		// Set the correct input format.
		TableInputFormat.registerDBInstance(instance);
		for (BulkProcessingOperator child : children) {
			TableInputFormat.addInputTable(job_conf, child.getResultTableName());
		}
		TableInputFormat.setNrOfInputSplitsPerTable(job_conf, 4);
		job.setInputFormatClass(AbstractExtensionFactory.getExtensionFactory().
				getTableInputFormat());
		
		// Set the correct output format.
		TableOutputFormat.registerDBInstance(instance);
		TableOutputFormat.setOutputTable(job_conf, getResultTableName());
		job.setOutputFormatClass(TableOutputFormat.class);
	}
	
	/**
	 * Configure the Hadoop job to be map only, i.e. the map output will be
	 * written directly to the output format.
	 * 
	 * @param mapper Mapper class that should be used.
	 */
	protected final void configureMapOnlyJob(
			Class<? extends Mapper<Text, DataTuple, ?, DataTuple>> mapper) {
		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataTuple.class);
		job.setNumReduceTasks(0);
		job_configured = true;
	}
	
	/**
	 * Configure the Hadoop job to use a full map-reduce pipeline.
	 * 
	 * @param mapper Mapper class that should be used.
	 * @param reducer Reducer class that should be used.
	 * @param mapOutputKey Key class that is emitted by the mapper.
	 * @param mapOutputValue Value class that is emitted by the mapper.
	 */
	protected final void configureMapReduceJob(
			Class<? extends Mapper<Text, DataTuple, K, V>> mapper, 
			Class<? extends Reducer<K, V, ?, DataTuple>> reducer,
			Class<K> mapOutputKey, Class<V> mapOutputValue) {
		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(mapOutputKey);
		job.setMapOutputValueClass(mapOutputValue);
		job.setReducerClass(reducer);
		job_configured = true;
	}
	
	/**
	 * Configure the Hadoop job to use a full map-reduce pipeline and also
	 * use a combiner between map and reduce.
	 * 
	 * @param mapper Mapper class that should be used.
	 * @param combiner Combiner class that should be used.
	 * @param reducer Reducer class that should be used.
	 * @param mapOutputKey Key class that is emitted by the mapper.
	 * @param mapOutputValue Value class that is emitted by the mapper.
	 */
	protected final void configureMapCombineReduceJob(
			Class<? extends Mapper<Text, DataTuple, K, V>> mapper,
			Class<? extends Reducer<K, V, K, V>> combiner,
			Class<? extends Reducer<K, V, ?, DataTuple>> reducer,
			Class<K> mapOutputKey, Class<V> mapOutputValue) {
		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(mapOutputKey);
		job.setMapOutputValueClass(mapOutputValue);
		job.setCombinerClass(combiner);
		job.setReducerClass(reducer);
		job_configured = true;
	}
	
	/**
	 * Run the configured MapReduce job.
	 */
	@Override
	protected boolean operatorMain() {
		if (!job_configured) {
			// Default to an identity map-only task.
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(DataTuple.class);
			job.setNumReduceTasks(0);
		}
		try {
			return job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
}
