package play.db.jpa;

/**
 *
 * @author dcardon
 */
public class InvalidDatabaseException extends RuntimeException
{
	/**
	 * Generated GUID for serialization support.
	 */
	private static final long serialVersionUID = -5099831811362344039L;
	
	public InvalidDatabaseException(String message)
	{
		super(message);
	}
}
