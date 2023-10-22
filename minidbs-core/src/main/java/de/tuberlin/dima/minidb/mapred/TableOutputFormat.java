package de.tuberlin.dima.minidb.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.io.cache.CacheableData;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TableResourceManager;

/**
 * OutputFormat that can write DataTuples to the MiniDBS table files. 
 * 
 * Note:
 * 	- Keys are discarded by this output format.
 * 
 * @author mheimel
 *
 */
public class TableOutputFormat extends OutputFormat<Writable, DataTuple> {

	// Registered database instance.
	private static DBInstance used_instance = null;
	
	/**
	 * Initializes the TableInputFormat by registering the currently active
	 * DB instance. This function needs to be called once before the 
	 * TableInputFormat can be used.
	 * 
	 * @param instance
	 */
	public static void registerDBInstance(DBInstance instance) {
		used_instance = instance;
	}
	
	// Configuration parameters.
	private final static String OUTPUT_TABLE_NAME = "dbms.output_table_name";
	private final static String OUTPUT_BATCH_SIZE = "dbms.output_batch_size";
	
	/**
	 * Registers the name of the output table that will be written by the 
	 * TableOutputFormat within the provided job configuration.
	 * 
	 * @param job_conf
	 * @param table_name
	 */
	public static void setOutputTable(Configuration job_conf, String table_name) {
		job_conf.set(OUTPUT_TABLE_NAME, table_name);
	}
	
	public static void setOutputBatchSize(Configuration job_conf, int batch_size) {
		job_conf.setInt(OUTPUT_BATCH_SIZE, batch_size);
	}
	

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		if (used_instance == null) {
			throw new RuntimeException("TableOutputFormat not initialized. "
					+ "Please call TableOutputFormat.registerDBInstance(..)!");
		}
		// Make sure that the output table is specified and exists.
		String out_table = context.getConfiguration().get(OUTPUT_TABLE_NAME);
		if (out_table == null) {
			throw new RuntimeException("Output table not specified. "
					+ "Please call TableOutputFormat.setOutputTable(..).");
		}
		if (used_instance.getCatalogue().getTable(out_table) == null) {
			throw new RuntimeException("Specified output table " + out_table + " does not exist.");
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// This only returns a dummy committer.
		return new OutputCommitter() {
			@Override
			public void setupTask(TaskAttemptContext arg0) throws IOException {}
			@Override
			public void setupJob(JobContext arg0) throws IOException {}
			@Override
			public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
				return false;
			}
			@Override
			public void commitTask(TaskAttemptContext arg0) throws IOException {}
			@Override
			public void abortTask(TaskAttemptContext arg0) throws IOException {}
		};
	}

	@Override
	public RecordWriter<Writable, DataTuple> getRecordWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		// Try to open the resource manager for the provided table.
		final String table_name = context.getConfiguration().get(OUTPUT_TABLE_NAME);
		final TableResourceManager res = used_instance.getCatalogue().
				getTable(table_name).getResourceManager();
		
		// Read the configuration parameters.
		final int batchSize = context.getConfiguration().getInt(OUTPUT_BATCH_SIZE, 16);
		
		return new RecordWriter<Writable, DataTuple>() {
			
			// Byte buffer to keep the content of a whole batch of buffers.
			private byte[][] pageBuffers = new byte[batchSize][res.getPageSize().getNumberOfBytes()];
			private int activePageInBuffer = -1;
			private int[] pageNumbers = new int[batchSize];
			
			private TablePage activePage = null;
			
			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				Thread io_thread = materializeActiveBuffers();
				if(io_thread != null) {
					io_thread.start();
					io_thread.join();
				}
			}
			
			private Thread materializeActiveBuffers() throws IOException {
				if (activePageInBuffer == -1) return null;
				// Make a copy of the buffers that will be materialized.
				final int actual_batch_size = activePageInBuffer + 1;
				final byte[][] buffers = new byte[actual_batch_size][];
				for (int i = 0; i < actual_batch_size; ++i) {
					buffers[i] = pageBuffers[i];
				}
				// Now extract the Cacheable data wrappers.
				final CacheableData wrappers[] = new CacheableData[actual_batch_size];
				for (int i=0; i<actual_batch_size; ++i) {
					final int pageNr = pageNumbers[i];
					wrappers[i] = new CacheableData() {
						@Override
						public void markExpired() {}
						@Override
						public boolean isExpired() {
							return false;
						}
						@Override
						public boolean hasBeenModified() throws PageExpiredException {
							return false;
						}
						@Override
						public int getPageNumber() throws PageExpiredException {
							return pageNr;
						}
						@Override
						public byte[] getBuffer() {
							return null;
						}
					};
				}
				// Now return a thread that will materialize the provided bufferrs.
				return new Thread(new Runnable() {
					@Override
					public void run() {
						try {
							res.writePagesToResource(buffers, wrappers);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
			}

			@Override
			public void write(Writable key, DataTuple tuple)
					throws IOException, InterruptedException {
				try {
					if (activePage == null || !activePage.insertTuple(tuple)) {
						// Allocate a new page.
						if (activePageInBuffer == (batchSize - 1)) {
							// Materialize the buffer.
							Thread io_thread = materializeActiveBuffers();
							io_thread.start();
							io_thread.join();
							// Now allocate new buffers.
							pageBuffers = new byte[batchSize][res.getPageSize().getNumberOfBytes()];
							activePageInBuffer = 0;
						} else {
							activePageInBuffer++;
						}
						// Build a new page.
						activePage = res.reserveNewPage(pageBuffers[activePageInBuffer]);
						pageNumbers[activePageInBuffer] = activePage.getPageNumber();
						// Try inserting into the new page.
						if (!activePage.insertTuple(tuple)) {
							throw new RuntimeException("Error inserting tuple into empty page. (Pagesize seems to small)");
						}
					}
				} catch (PageFormatException e) {
					throw new RuntimeException(e);
				}
			}
		};

	}

}
