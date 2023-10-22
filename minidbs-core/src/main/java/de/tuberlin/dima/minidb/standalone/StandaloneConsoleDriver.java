package de.tuberlin.dima.minidb.standalone;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.tuberlin.dima.minidb.Constants;
import de.tuberlin.dima.minidb.DBInstance;
import de.tuberlin.dima.minidb.ResultHandler;
import de.tuberlin.dima.minidb.api.AbstractExtensionFactory;
import de.tuberlin.dima.minidb.api.ExtensionInitFailedException;
import de.tuberlin.dima.minidb.tracing.ConsoleMessageFormatter;
import de.tuberlin.dima.minidb.tracing.TraceMessageFormatter;


/**
 * This class represents the standalone driver for the MiniDBS accepting statements from
 * the standard input stream.
 * It acts as the main entry-point for the MiniDBS console standalone application.
 *  
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class StandaloneConsoleDriver
{
	// --------------------------------------------------------------------------------------------
	//                                        Constants
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The string describing the valid syntax to call the program.
	 */
	private static final String USAGE = "MiniDBS [-options]\n\n" +
			"where options include:\n" +
			"  -config <file>           The configuration file to use.\n" +
			"  -catalogue <file>        The catalogue file to use.\n" +
			"  -extension <class-name>  The extension factory to use.\n" +
			"  -log <log-level>         The log granularity (SEVERE, " + 
			                            "WARNING, INFO, CONFIG, FINE, ALL)\n" ;
	
	/**
	 * The name of the root logger for this database system.
	 */
	private static final String GLOBAL_LOGGER_NAME = "MINIDBS-LOGGER";

	
	
	// --------------------------------------------------------------------------------------------
	//                                    MAIN ENTRY POINT
	// --------------------------------------------------------------------------------------------
	
	/**
	 * The main entry point method for the MiniDBS standalone driver. This method
	 * accepts the command line parameters and evaluates them.
	 * 
	 * The return codes are to be interpreted the following way:
	 * <ul>
	 *   <li>0: Normal operation, everything in order.</li>
	 *   <li>1: System did not start, because some command line arguments were unknown.</li>
	 *   <li>2: System did not start because of invalid config parameters
	 *          (config, catalogue, ...)</li>
	 *   <li>3: System found corrupt data files (table or index resources, etc.).</li>
	 *   <li>4: System quit operation because the underlying OS or hardware reported a
	 *          problem.</li>
	 *   <li>5: The system encountered an internal problem or bug.</li>
	 * </ul> 
	 * 
	 * @param args The array of command line parameters.
	 * @throws IOException 
	 */
	public static void main(String[] args)
	{
		// set up the logger.
		// by default, warning and severe messages go to the console, and info
		// messages go in addition to a message log file
		Logger logger = Logger.getLogger(GLOBAL_LOGGER_NAME);
		logger.setLevel(Level.INFO);
		logger.setUseParentHandlers(false);
		
		Handler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(Level.WARNING);
		consoleHandler.setFormatter(new ConsoleMessageFormatter());
		logger.addHandler(consoleHandler);
		
		Handler logFileHandler = null;
        try {
    		logFileHandler = new FileHandler("messages", 4096*1024, 10, true);
			logFileHandler.setLevel(Level.INFO);
			logFileHandler.setFormatter(new TraceMessageFormatter());
			logger.addHandler(logFileHandler);
        }
        catch (Exception e) {
        	System.err.println(
        			"Cannot open trace log file. No log will be created during this session.");
        }

		
		// start with the default values for config and catalogue file
		// as well as extension factory class
		String configFileName = Constants.CONFIG_FILE_PATH;
		String catalogueFileName = Constants.CATALOGUE_FILE_PATH;
		
		// parse the arguments
		for (int i = 0; i < args.length; i++)
		{
			String arg = args[i];
			if (i < args.length - 1) {
				// two more arguments available
				if (arg.equalsIgnoreCase("-config")) {
					i++;
					configFileName = args[i];
					continue;
				}
				else if (arg.equalsIgnoreCase("-catalogue")) {
					i++;
					catalogueFileName = args[i];
					continue;
				}
//				else if (arg.equalsIgnoreCase("-extension")) {
//					i++;
//					extensionFactoryClass = args[i];
//					continue;
//				}
				else if (arg.equalsIgnoreCase("-log")) {
					i++;
					try {
						Level l = Level.parse(args[i]);
						if (l.intValue() < Level.WARNING.intValue()) {
							logger.setLevel(l);
						}
						else {
							logger.setLevel(Level.WARNING);
						}
						logFileHandler.setLevel(l);
					}
					catch (IllegalArgumentException iaex) {
						System.err.println("Invalid log level: " + args[i]);
						System.exit(DBInstance.RETURN_CODE_UNKNOWN_COMMAND_LINE_ARGUMENT);
					}
					
					continue;
				}
			}
			
			// argument is not part of a two string parameter

			// unrecognized argument
			System.err.println(USAGE);
			System.exit(DBInstance.RETURN_CODE_UNKNOWN_COMMAND_LINE_ARGUMENT);
		}
		
		// initialize the extension factory.
		try {
			AbstractExtensionFactory.initializeDefault();
//			if (extensionFactoryClass == null) {
//				AbstractExtensionFactory.initializeDefault();
//			}
//			else {
//				AbstractExtensionFactory.initFromClassName(extensionFactoryClass);
//			}
		}
		catch (ExtensionInitFailedException eifex) {
			logger.log(Level.SEVERE, "The extension factory could not be initialized: " +
					eifex.getMessage(), eifex);
			System.exit(DBInstance.RETURN_CODE_SYSTEM_PROBLEM);
			return;
		}
		
		// parameters are all available, create the instance
		DBInstance instance = null;
		try {
			instance = new DBInstance(logger, configFileName, catalogueFileName);
		}
		catch (Exception ex) {
			logger.log(Level.SEVERE, "An uncategorized error occurred creating the instance: " +
					ex.getMessage(), ex);
			System.exit(DBInstance.RETURN_CODE_INTERNAL_PROBLEM);
			return;
		}
		
		// now start the instance. this will acquire all resources
		int code = instance.startInstance();
		if (code != DBInstance.RETURN_CODE_OKAY) {
			System.exit(code);
		}
		
		// system is started, now listen for statements
		logger.info("MiniDBS standalone system up and running.");
		System.out.println("System running. Enter SQL query, or enter 'exit' to quit...");
		
		// read from standard in until the 'exit' keyword comes.
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String line = null;
		try {
	        while (instance.isRunning() && (line = reader.readLine()) != null)
	        {
	        	// check for empty lines
	        	if (line.length() == 0) {
	        		continue;
	        	}
	        	
	        	// check for exit keyword
	        	if (line.equalsIgnoreCase("exit")) {
	        		break;
	        	}
	        	
	        	System.out.println();
	        	
	        	// create a result set that prints to the console and invoke the instance
	        	// to process the query
	        	ResultHandler set = new PrintStreamResultSet(System.out);
	        	instance.processQuery(line, set);
	        	
	        	System.out.println();
	        }
        }
        catch (Exception e) {
        	System.err.println("An unexpected error occurred: " + e.getMessage());
        	e.printStackTrace();
        }
		
        // shut the system down if it did not terminate due to an error
        int exitCode;
        if (instance.isRunning()) {
	        // system will be shut down
	        System.out.println("Shutting down the system...");
			
			// system is to be shut down
			exitCode = instance.shutdownInstance();
			logger.info("System shut down with code " + exitCode);
        }
        else {
        	exitCode = DBInstance.RETURN_CODE_SYSTEM_PROBLEM;
        }
		
		// close the logger
		logger.removeHandler(logFileHandler);
		logFileHandler.close();
		
		System.exit(exitCode);
	}
}
