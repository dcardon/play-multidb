package play.db.jpa;

import play.mvc.Http.Request;

/**
 *	The default request database key extractor.  This class is used if none 
 *	is found in the application.
 * @author dcardon
 */
public class DomainDBKeyExtractor implements RequestDBKeyExtractor
{
	/**
	 * Extracts the database key from the request object.
	 * @param request
	 * @return
	 */
	@Override
	public String extractKey(Request request)
	{
		//
		//	The domain is the key.
		//
		return (request == null ? null : request.domain);
	}
}
