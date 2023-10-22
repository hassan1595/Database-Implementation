package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.core.DataTuple;

/**
 * Abstract class template to implement an InputFormat that can read DMBS
 * tables.
 * 
 * Note:
 *  Since we rely on passing a specific DBInstance object, this input format
 *  will only work for local Hadoop jobs.	
 * 
 * @author mheimel
 *
 */
public abstract class TableInputFormat extends InputFormat<Text, DataTuple> {

	/**
	 * Used to pass the current DBinstance to the input format.
	 */
	private static DBInstance instance;
	public static void registerDBInstance(DBInstance instance) {
		TableInputFormat.instance = instance;
	}
	/**
	 * Use this function to access the instance from deriving classes.
	 * @return The currently registered instance.
	 */
	protected static DBInstance getDBInstance() {
		return instance;
	}
	
	/**
	 * Required configuration parameters for the TableInputFormat.
	 */
	protected final static String INPUT_TABLE_NAMES = "dbms.input_table_name";
	protected final static String INPUT_SPLIT_SIZE = "dbms.pages_per_input_split";
	protected final static String SPLITS_PER_TABLE = "dbms.input_splits_per_table";
	protected final static String INPUT_PREFETCH_WINDOW = "dbms.input_prefetch_window";
	
	/**
	 * Registers the name of an input table that should be read by the 
	 * TableInputFormat within the provided job configuration.
	 * 
	 * @param job_conf
	 * @param table_name
	 */
	public static void addInputTable(Configuration job_conf, 
			String table_name) {
		// Construct a comma-separated list of input tables.
		String table_names = job_conf.get(INPUT_TABLE_NAMES);
		table_names = (table_names == null) ? 
				table_name : (table_names + "," + table_name);
		job_conf.set(INPUT_TABLE_NAMES, table_names);
	}
	
	/**
	 * Specifies the size of the splits that are generated.
	 * 
	 * Note: 
	 * 	This call reverts all effects of setNrOfInputSplitsPerTable(...).
	 * 
	 * @param job_conf
	 * @param pages_per_split
	 */
	public static void setInputSplitSize(Configuration job_conf,
			int pages_per_split) {
		job_conf.unset(SPLITS_PER_TABLE);
		job_conf.setInt(INPUT_SPLIT_SIZE, pages_per_split);
	}
	
	/**
	 * Specifies how many splits are generated for each input table.
	 * 
	 * Note:
	 * 	This call reverts all effects of setInputSplitSize(...).
	 * 
	 * @param job_conf
	 * @param nr_of_splits
	 */
	public static void setNrOfInputSplitsPerTable(Configuration job_conf, 
			int nr_of_splits) {
		job_conf.unset(INPUT_SPLIT_SIZE);
		job_conf.setInt(SPLITS_PER_TABLE, nr_of_splits);
	}
	
	/**
	 * Specifies how large (in pages) the prefetch window for table access should be.
	 * 
	 * @param job_conf
	 * @param window_size
	 */
	public static void setInputPrefetchWindow(Configuration job_conf, 
			int window_size) {
		job_conf.setInt(INPUT_PREFETCH_WINDOW, window_size);
	}
	
	/**
	 * Creates a new RecordReader to read tuples from the current split.
	 * 
	 * By convention, the RecordReader should return Key Value Pairs that have
	 * the Key set to be the table name and the value to be the DataTuple. The
	 * record reader should use the BufferPoolManager of the registered database
	 * instance to access tablepages.
	 * 
	 * The record reader should use the configuration value in TableInputFormat.
	 * INPUT_PREFETCH_WINDOW to decide how many pages to prefetch.
	 */
	@Override
	public abstract RecordReader<Text, DataTuple> createRecordReader(
			InputSplit split, TaskAttemptContext Context) 
					throws IOException, InterruptedException;

	/**
	 * Build the list of all Inputsplits that have to be read.
	 * 
	 * Note:
	 * 	This function should take the configuration values of TableInputFormat.
	 *  INPUT_TABLE_NAMES, TableInputFormat.SPLITS_PER_TABLE and TableInputFormat.
	 *  INPUT_SPLIT_SIZE into account. In particular:
	 *  
	 *  - TableInputFormat.INPUT_SPLIT_NAMES contains a comma-separated list of
	 *    the names of the tables that should be loaded.
	 *  - If TableInputFormat.SPLITS_PER_TABLE is set, you should return exactly
	 *    SPLITS_PER_TABLE splits for each table. The only exception is if a 
	 *    table has fewer pages than the number of requested splits. In this 
	 *    case, it is ok to return fewer splits.
	 *  - If TableInputFormat.INPUT_SPLIT_SIZE is set, you should return splits
	 *    that have exactly INPUT_SPLIT_SIZE pages. The only exception is the 
	 *    last split for a table (can be smaller to account for remainders), or 
	 *    if a table has not enough pages to fill one split.
	 *  - Note: You can assume that either INPUT_SPLIT_SIZE or SPLITS_PER_TABLE
	 *    is set, but never both at the same time.
	 *  
	 *  Please note that splits should only comprise full tablepages of a single
	 *  table. This means you should never:
	 *     - build a split that contains pages of multiple tables.
	 *     - split Tablepages across splits.
	 */
	@Override
	public abstract List<InputSplit> getSplits(JobContext context) 
			throws IOException, InterruptedException;

}
