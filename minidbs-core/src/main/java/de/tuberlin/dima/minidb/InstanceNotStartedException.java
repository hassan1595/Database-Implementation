package de.tuberlin.dima.minidb;


/**
 * An exception signifying that some resource is accessed that is not available because
 * the system has not properly started up. 
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class InstanceNotStartedException extends RuntimeException
{
	/**
	 * The serial version UID for serialization compatibility.
	 */
    private static final long serialVersionUID = 3418245009361979540L;

	/**
	 * Create a plain SystemStartupIncompleteException with a default message.
	 */
	public InstanceNotStartedException()
	{
		super("The system startup has not propery finished.");
	}
}
