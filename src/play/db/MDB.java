package play.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;
import play.db.jpa.JPA;
import play.exceptions.DatabaseException;

/**
 * Multiple database connection utilities.
 */
public class MDB
{

	/**
	 * The loaded datasources.  This accepts a 
	 */
	public static Map<String, DataSource> datasources = null;

	/**
	 * Close the connection opened for the current thread.
	 */
	public static void close()
	{
		if (localConnection.get() != null)
		{
			try
			{
				Connection connection = localConnection.get();
				localConnection.set(null);
				connection.close();
			}
			catch (Exception e)
			{
				throw new DatabaseException(
						"It's possible than the connection was not propertly closed !", e);
			}
		}
	}

	/**
	 * The connection for the current thread.
	 */
	static ThreadLocal<Connection> localConnection = new ThreadLocal<Connection>();

	/**
	 * Open a connection for the current thread.
	 * 
	 * @return A valid SQL connection
	 */
	@SuppressWarnings("deprecation")
	public static Connection getConnection(String dbKey)
	{
		try
		{
			if (JPA.isEnabled())
			{
				return ((org.hibernate.ejb.EntityManagerImpl) JPA.em()).getSession().connection();
			}
			if (localConnection.get() != null)
			{
				return localConnection.get();
			}
			Connection connection = datasources.get(dbKey).getConnection();
			localConnection.set(connection);
			return connection;
		}
		catch (SQLException ex)
		{
			throw new DatabaseException("Cannot obtain a new connection ("
					+ ex.getMessage() + ")", ex);
		}
		catch (NullPointerException e)
		{
			if (datasources.get(dbKey) == null)
			{
				throw new DatabaseException(
						"No database found under key '" + dbKey + "'. Check the configuration of your application.",
						e);
			}
			throw e;
		}
	}

	/**
	 * Execute an SQL update
	 * 
	 * @param SQL
	 * @return false if update failed
	 */
	public static boolean execute(String SQL)
	{
		try
		{
			return getConnection(null).createStatement().execute(SQL);
		}
		catch (SQLException ex)
		{
			throw new DatabaseException(ex.getMessage(), ex);
		}
	}

	/**
	 * Execute an SQL query
	 * 
	 * @param SQL
	 * @return The query resultSet
	 */
	public static ResultSet executeQuery(String SQL)
	{
		try
		{
			return getConnection(null).createStatement().executeQuery(SQL);
		}
		catch (SQLException ex)
		{
			throw new DatabaseException(ex.getMessage(), ex);
		}
	}
}
