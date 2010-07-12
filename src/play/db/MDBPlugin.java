package play.db;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import play.Logger;
import play.Play;
import play.PlayPlugin;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * The MDB plugin.  This plugin allows multiple databases to be defined for use by play and its transactions.
 */
public class MDBPlugin extends PlayPlugin
{

	private static final String MDB_ALL_KEY = "all";
	private static final String MDB_CONF_PREFIX = "mdb.";
	private static final String MDB_DRIVER_PREFIX = MDB_CONF_PREFIX + "driver.";
	private static final String MDB_URL_PREFIX = MDB_CONF_PREFIX + "url.";
	private static final String MDB_USER_PREFIX = MDB_CONF_PREFIX + "user.";
	private static final String MDB_PASS_PREFIX = MDB_CONF_PREFIX + "pass.";
	private static final String MDB_POOL_TIMEOUT_PREFIX = MDB_CONF_PREFIX + "pool.timeout.";
	private static final String MDB_POOL_MAX_PREFIX = MDB_CONF_PREFIX + "pool.maxSize.";
	private static final String MDB_POOL_MIN_PREFIX = MDB_CONF_PREFIX + "pool.minSize.";
	private static final String MDB_KEY_PREFIX = MDB_CONF_PREFIX + "key.";

	@Override
	public void onApplicationStart()
	{
		if (changed())
		{
			MDB.datasources = new HashMap<String, DataSource>();
			Map<String, DbParameters> dbMap = new HashMap<String, DbParameters>();
			try
			{
				dbMap = extractDbParameters();
			}
			catch (Exception e)
			{
				Logger.error(e, "Error collecting data for multiple database plugin");
			}
			
			DbParameters allEntry = dbMap.get(MDB_ALL_KEY);
			if (allEntry == null)
			{
				allEntry = new DbParameters();
			}
			
			for (Entry<String, DbParameters> parm : dbMap.entrySet())
			{

				try
				{
					//
					//	Don't connect to the 'all' entry.
					//
					if (MDB_ALL_KEY.equals(parm.getKey()))
					{
						continue;
					}
					
					//
					//	Substitute entries from the 'all' entry.
					//
					DbParameters db = parm.getValue();
					db.inherit(allEntry);
					
					//
					//	Try to connect.
					//
					makeConnection(db);
				}
				catch (Exception e)
				{
					Logger.error(e, "Cannot connect to the database [" + parm.getKey() + "]: %s", e.getMessage());
				}
			}
		}
	}

	/**
	 * Extracts the database parameters from the configuration file.
	 * @param dbMap
	 * @return 
	 */
	private static Map<String, DbParameters> extractDbParameters()
	{
		Map<String, DbParameters> dbMap = new HashMap<String, DbParameters>();
		Properties p = Play.configuration;
		for (Entry<Object, Object> entry : p.entrySet())
		{
			if (entry.getKey() instanceof String)
			{
				String propKey = (String) entry.getKey();
				if (propKey == null || !propKey.startsWith(MDB_CONF_PREFIX))
				{
					continue;
				}
				
				String mapKey = StringUtils.substringAfterLast(propKey, ".");
				DbParameters mapEntry = dbMap.get(mapKey);
				if (mapEntry == null)
				{
					mapEntry = new DbParameters();
					dbMap.put(mapKey, mapEntry);
				}
				
				if (propKey.startsWith(MDB_DRIVER_PREFIX))
				{
					mapEntry.driver = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_DRIVER_PREFIX))
				{
					mapEntry.driver = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_KEY_PREFIX))
				{
					mapEntry.key = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_URL_PREFIX))
				{
					mapEntry.url = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_USER_PREFIX))
				{
					mapEntry.user = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_PASS_PREFIX))
				{
					mapEntry.pass = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_POOL_TIMEOUT_PREFIX))
				{
					mapEntry.poolTimeout = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_POOL_MAX_PREFIX))
				{
					mapEntry.poolMaxSize = (String) entry.getValue();
				}
				else if (propKey.startsWith(MDB_POOL_MIN_PREFIX))
				{
					mapEntry.poolMinSize = (String) entry.getValue();
				}
				else
				{
					Logger.warn("Unrecognized MDB key: " + propKey);
				}
			}
			else
			{
				Logger.warn("Unexpected non-string property key: " + entry.getKey());
			}
		}
		
		return dbMap;
	}

	@Override
	public String getStatus()
	{
		StringWriter sw = new StringWriter();
		PrintWriter out = new PrintWriter(sw);
		out.println("         Multiple DB sources:");
		out.println("=================================================");
		
		if (MDB.datasources == null || MDB.datasources.isEmpty())
		{
			out.println("Datasources:");
			out.println("~~~~~~~~~~~");
			out.println("(not yet connected)");
			return sw.toString();
		}
		
		for (Entry<String, DataSource> entry : MDB.datasources.entrySet())
		{
			if (entry == null || entry.getValue() == null || !(entry.getValue() instanceof ComboPooledDataSource))
			{
				out.println("Datasource [" + entry.getKey() + "]:");
				out.println("~~~~~~~~~~~");
				out.println("(not yet connected)");
				continue;
			}
			ComboPooledDataSource datasource = (ComboPooledDataSource) entry.getValue();
			out.println("Datasource [" + entry.getKey() + "]:");
			out.println("~~~~~~~~~~~");
			out.println("Jdbc url: " + datasource.getJdbcUrl());
			out.println("Jdbc driver: " + datasource.getDriverClass());
			out.println("Jdbc user: " + datasource.getUser());
			out.println("Jdbc password: " + datasource.getPassword());
			out.println("Min pool size: " + datasource.getMinPoolSize());
			out.println("Max pool size: " + datasource.getMaxPoolSize());
			out.println("Initial pool size: " + datasource.getInitialPoolSize());
			out.println("Checkout timeout: " + datasource.getCheckoutTimeout());
			out.println("");
		}
		out.println("=================================================");
		return sw.toString();
	}

	@Override
	public void invocationFinally()
	{
		MDB.close();
	}

	/**
	 * Database parameters, which are needed for each database.
	 * 
	 * @author dcardon
	 */
	private static class DbParameters
	{
		public String key;
		public String url;
		public String driver;
		public String user;
		public String pass;
		public String poolTimeout;
		public String poolMaxSize;
		public String poolMinSize;
		public void inherit(DbParameters allEntry)
		{
			this.driver = StringUtils.defaultIfEmpty(this.driver, allEntry.driver);
			this.pass = StringUtils.defaultIfEmpty(this.pass, allEntry.pass);
			this.user = StringUtils.defaultIfEmpty(this.user, allEntry.user);
			this.poolMaxSize = StringUtils.defaultIfEmpty(this.poolMaxSize, allEntry.poolMaxSize);
			this.poolMinSize = StringUtils.defaultIfEmpty(this.poolMinSize, allEntry.poolMinSize);
			this.poolTimeout = StringUtils.defaultIfEmpty(this.poolTimeout, allEntry.poolTimeout);
			this.url = StringUtils.defaultIfEmpty(this.url, allEntry.url);
		}
	}

	/**
	 * Creates a connection using the passed database parameters.
	 * @param parms
	 * @throws Exception
	 */
	private void makeConnection(DbParameters parms) throws Exception
	{
		// Try the driver
		String driver = parms.driver;
		try
		{
			Driver d = (Driver) Class.forName(driver, true, Play.classloader).newInstance();
			DriverManager.registerDriver(new DBPlugin.ProxyDriver(d));
		}
		catch (Exception e)
		{
			throw new Exception("Driver not found (" + driver + ")");
		}

		// Try the connection
		Connection fake = null;
		try
		{
			if (parms.user == null)
			{
				fake = DriverManager.getConnection(parms.url);
			}
			else
			{
				fake = DriverManager.getConnection(parms.url, parms.user, parms.pass);
			}
		}
		finally
		{
			if (fake != null)
			{
				fake.close();
			}
		}

		System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
		System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "OFF");
		ComboPooledDataSource ds = new ComboPooledDataSource();
		ds.setDriverClass(parms.driver);
		ds.setJdbcUrl(parms.url);
		ds.setUser(parms.user);
		ds.setPassword(parms.pass);
		ds.setAcquireRetryAttempts(1);
		ds.setAcquireRetryDelay(0);
		ds.setCheckoutTimeout(Integer.parseInt(StringUtils.defaultIfEmpty(parms.poolTimeout, "5000")));
		ds.setBreakAfterAcquireFailure(true);
		ds.setMaxPoolSize(Integer.parseInt(StringUtils.defaultIfEmpty(parms.poolMaxSize, "30")));
		ds.setMinPoolSize(Integer.parseInt(StringUtils.defaultIfEmpty(parms.poolMinSize, "1")));
		ds.setTestConnectionOnCheckout(true);
		MDB.datasources.put(parms.key, ds);
		Connection c = null;
		try
		{
			c = ds.getConnection();
		}
		finally
		{
			if (c != null)
			{
				c.close();
			}
		}
		Logger.info("Connected to %s", ds.getJdbcUrl());
	}

	/**
	 * Determine if the datasource(s) have changed.
	 * @return
	 */
	private static boolean changed()
	{
		Map<String, DbParameters> dbMap = extractDbParameters();
		//
		//	Get the 'all' entry
		//
		DbParameters allEntry = dbMap.get(MDB_ALL_KEY);
		if (allEntry == null)
		{
			allEntry = new DbParameters();
		}
		
		boolean hasChanged = false;
		
		//
		//	Loop over all entries to see if a change has occurred.
		//
		for (Entry<String, DbParameters> parm : dbMap.entrySet())
		{
			if (MDB.datasources == null || MDB.datasources.isEmpty())
			{
				return true;
			}
			
			//
			//	Skip the 'all' entry
			//
			if (MDB_ALL_KEY.equals(parm.getKey()))
			{
				continue;
			}
			
			DbParameters db = parm.getValue();
			
			//
			//	Ignore nulls.
			//
			if (db == null || (db.driver == null) || (db.url == null))
			{
				continue;
			}
			
			ComboPooledDataSource ds = (ComboPooledDataSource) MDB.datasources.get(parm.getKey());
			if (ds == null)
			{
				hasChanged |= true;
			}
			else
			{
				if (!StringUtils.defaultString(db.driver).equals(ds.getDriverClass()))
				{
					hasChanged |= true;
				}
				if (!StringUtils.defaultString(db.url).equals(ds.getJdbcUrl()))
				{
					hasChanged |= true;
				}
				if (!StringUtils.defaultString(db.user).equals(ds.getUser()))
				{
					hasChanged |= true;
				}
				if (!StringUtils.defaultString(db.pass).equals(ds.getPassword()))
				{
					hasChanged |= true;
				}
			}
		}
		
		return hasChanged;
	}
}
