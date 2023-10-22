package de.tuberlin.dima.minidb.mapred.qexec.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.mapred.qexec.BulkProcessingOperator;
import de.tuberlin.dima.minidb.mapred.qexec.HadoopOperator;

/**
 * An exemplary multi-input operator that always forwards its left child.
 * 
 * This operator should demonstrate how to integrate custom Mapper classes, how
 * to pass parameters to them and how to deal with multiple inputs.
 * 
 * @author mheimel
 *
 */
public class ForwardLeftInputOperator extends HadoopOperator<Text, DataTuple> {

	/**
	 * A mapper class that selectively forwards only those tuples that have a corresponding
	 * key set.
	 * 
	 * Note: Inline classes must be static if they are passed as Mappers / Reducers to HadoopJob.
	 * 
	 * @author mheimel
	 *
	 */
	public static class SelectiveForwarderMapper extends Mapper<Text, DataTuple, Text, DataTuple> {
		
		/**
		 * Name of the configuration option that is used to store the selected key.
		 */
		private static final String SELECTED_KEY_OPTION = "SelectiveForwarderMapper.selected_key";
	
		/**
		 * Specify the key that will be forwarded by this Mapper.
		 * 
		 * @param jobConf
		 * @param key
		 */
		public static void specifySelectedKey(Configuration jobConf, String key) {
			jobConf.set(SELECTED_KEY_OPTION, key);
		}
		
		public Text selected_key;
		
		@Override
		public void setup(Context context) {
			String selected_key_string = context.getConfiguration().get(SELECTED_KEY_OPTION, "");
			if (selected_key_string == "") {
				throw new RuntimeException("No key specified!");
			}
			selected_key = new Text(selected_key_string);
		}
		
		@Override
		public void map(Text key, DataTuple value, Context context) 
				throws IOException, InterruptedException {
			if (key.equals(selected_key)) {
				context.write(key, value);
			}
		}
	
	}
	
	private BulkProcessingOperator left_child;
	
	public ForwardLeftInputOperator(DBInstance dbInstance, 
			BulkProcessingOperator left_child, 
			BulkProcessingOperator right_child) throws IOException {
		super(dbInstance, left_child, right_child);
		// We only need to run a job that uses a single mapper.
		configureMapOnlyJob(SelectiveForwarderMapper.class);
		SelectiveForwarderMapper.specifySelectedKey(
				job.getConfiguration(), 
				left_child.getResultTableName());
		this.left_child = left_child;
	}

	@Override
	protected TableSchema createResultSchema() {
		return left_child.getResultSchema();
	}

}
