package de.tuberlin.dima.minidb.io.manager;

/**
 * The abstract superclass of all classes that describe entries for an I/O queue. This superclass
 * generically describes a page I/O task and describes the resourceId for that task, the page number
 * and allows to track exceptions.
 * Two I/O queue entries are considered equivalent, if they describe the same resourceId and page number.
 *
 * @author Rudi Poepsel Lemaitre (r.poepsellemaitre@tu-berlin.de)
 */
public abstract class QueueEntry implements Comparable<QueueEntry> {
	/**
	 * The resourceId manager to access the resourceId.
	 */
	private final ResourceManager resourceManager;

	/**
	 * The resourceId to perform the I/O on.
	 */
	private final Integer resourceId;

	/**
	 * The page number of the page that is target of the I/O operation.
	 */
	private final int pageNumber;

	/**
	 * Initializes a new QueueEntry for the given I/O task.
	 *
	 * @param resourceId
	 *        The resourceId to perform the I/O on.
	 * @param pageNumber
	 *        The page number of the page that is target of the I/O operation.
	 * @param resourceManager
	 *        The resourceId manager to access the resourceId.
	 */
	public QueueEntry(Integer resourceId, int pageNumber, ResourceManager resourceManager) {
		this.resourceManager = resourceManager;
		this.resourceId = resourceId;
		this.pageNumber = pageNumber;
	}

	/**
	 * Gets the resourceId of this QueueEntry.
	 *
	 * @return the resourceId
	 */
	public Integer getResourceId() {
		return this.resourceId;
	}

	/**
	 * Gets the pageNumber of this QueueEntry.
	 *
	 * @return the pageNumber
	 */
	public int getPageNumber() {
		return this.pageNumber;
	}

	/**
	 * Gets the resourceManager of this QueueEntry.
	 *
	 * @return the resourceManager
	 */
	public ResourceManager getResourceManager() {
		return this.resourceManager;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(QueueEntry o) {
		if (o.resourceId.equals(this.resourceId)) {
			return this.pageNumber - o.pageNumber;
		} else {
			throw new IllegalArgumentException();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof QueueEntry)) {
			return false;
		}

		QueueEntry other = (QueueEntry) obj;
		return (this.pageNumber == other.pageNumber && this.resourceId.equals(other.resourceId));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 31 * (31 + this.pageNumber) + this.resourceId.hashCode();
	}


}
