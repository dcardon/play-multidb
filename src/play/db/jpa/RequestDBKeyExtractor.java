package play.db.jpa;

import play.mvc.Http.Request;

/**
 *	Extracts the datbaase key from the request.  This interface should be implemented in order to
 *	produce the key which identifies the database to which we should connect. 
 * @author dcardon
 */
public interface RequestDBKeyExtractor
{
	/**
	 * Extracts the database key from the request.
	 * @param request
	 * @return
	 */
	public String extractKey(Request request);
}
