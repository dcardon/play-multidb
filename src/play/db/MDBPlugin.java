package play.db;

import java.beans.PropertyVetoException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.persistence.Entity;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.hibernate.ejb.Ejb3Configuration;

import play.Logger;
import play.Play;
import play.PlayPlugin;
import play.db.jpa.JPA;
import play.db.jpa.MJPAPlugin;
import play.exceptions.JPAException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * The MDB plugin.  This plugin allows multiple databases to be defined for use by play and its transactions.
 */
public class MDBPlugin extends PlayPlugin
{

	private static final String MDB_ALL_KEY = "all";
	private static final String MDB_CONF_PREFIX = "mdb.";
	public static final String MDB_DRIVER_PREFIX = MDB_CONF_PREFIX + "driver.";
	public static final String MDB_URL_PREFIX = MDB_CONF_PREFIX + "url.";
	public static final String MDB_USER_PREFIX = MDB_CONF_PREFIX + "user.";
	public static final String MDB_PASS_PREFIX = MDB_CONF_PREFIX + "pass.";
	public static final String MDB_POOL_TIMEOUT_PREFIX = MDB_CONF_PREFIX + "pool.timeout.";
	public static final String MDB_POOL_MAX_PREFIX = MDB_CONF_PREFIX + "pool.maxSize.";
	public static final String MDB_POOL_MIN_PREFIX = MDB_CONF_PREFIX + "pool.minSize.";
	public static final String MDB_KEY_PREFIX = MDB_CONF_PREFIX + "key.";

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
				
				applyParameter((String) entry.getValue(), propKey, mapEntry);
			}
			else
			{
				Logger.warn("Unexpected non-string property key: " + entry.getKey());
			}
		}
		
		return dbMap;
	}

	/**
	 * @param entry
	 * @param propKey
	 * @param mapEntry
	 */
	private static void applyParameter(String propValue, String propKey,
			DbParameters mapEntry)
	{
		if (propKey.startsWith(MDB_DRIVER_PREFIX))
		{
			mapEntry.driver = propValue;
		}
		else if (propKey.startsWith(MDB_KEY_PREFIX))
		{
			mapEntry.key = propValue;
		}
		else if (propKey.startsWith(MDB_URL_PREFIX))
		{
			mapEntry.url = propValue;
		}
		else if (propKey.startsWith(MDB_USER_PREFIX))
		{
			mapEntry.user = propValue;
		}
		else if (propKey.startsWith(MDB_PASS_PREFIX))
		{
			mapEntry.pass = propValue;
		}
		else if (propKey.startsWith(MDB_POOL_TIMEOUT_PREFIX))
		{
			mapEntry.poolTimeout = propValue;
		}
		else if (propKey.startsWith(MDB_POOL_MAX_PREFIX))
		{
			mapEntry.poolMaxSize = propValue;
		}
		else if (propKey.startsWith(MDB_POOL_MIN_PREFIX))
		{
			mapEntry.poolMinSize = propValue;
		}
		else
		{
			Logger.warn("Unrecognized MDB key: " + propKey);
		}
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
	private static void makeConnection(DbParameters parms) throws Exception
	{
		ComboPooledDataSource ds = makeDatasource(parms);
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
	 * @param parms
	 * @return
	 * @throws Exception
	 * @throws SQLException
	 * @throws PropertyVetoException
	 */
	private static ComboPooledDataSource makeDatasource(DbParameters parms)
			throws Exception, SQLException, PropertyVetoException
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
		return ds;
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
	
	/**
	 * Adds a database to the application server while it's running.
	 * @param dbParms
	 */
	@SuppressWarnings("unchecked")
	public static void addDatabase(Map<String, String> dbParms)
	{
		Map<String, DbParameters> dbMap = extractDbParameters();
		DbParameters allEntry = dbMap.get(MDB_ALL_KEY);
		if (allEntry == null)
		{
			allEntry = new DbParameters();
		}
		
		DbParameters dbParm = new DbParameters();
		for (Entry<String, String> entry : dbParms.entrySet())
		{
			applyParameter(entry.getValue(), entry.getKey(), dbParm);
		}
		
		//
		//	Inherit from the 'all' entry.
		//
		dbParm.inherit(allEntry);
		
		try
		{
			ComboPooledDataSource ds = makeDatasource(dbParm);
			synchronized (MDB.datasources)
			{
				MDB.datasources.put(dbParm.key, ds);
			}
			Connection c = null;
			try
			{
				c = ds.getConnection();
				
				List<Class> classes = Play.classloader.getAnnotatedClasses(Entity.class);
				if (classes.isEmpty()
						&& Play.configuration.getProperty("jpa.entities", "").equals(""))
				{
					return;
				}
				//
				//	Now, add the datasource into the MJPAPlugin
				//
				Ejb3Configuration cfg = MJPAPlugin.buildEjbConfiguration(classes, ds);
				
				Logger.trace("Initializing JPA ...");
				try
				{
					EntityManagerFactory factory = cfg.buildEntityManagerFactory(); 
					JPA.entityManagerFactory = factory;
					
					synchronized (MJPAPlugin.factoryMap)
					{
						MJPAPlugin.factoryMap.put(dbParm.key, factory);
					}
				}
				catch (PersistenceException e)
				{
					throw new JPAException(e.getMessage(), e.getCause() != null
							? e.getCause() : e);
				}
				
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
		catch (Exception e)
		{
			Logger.error(e, "Error adding database: " + dbParms);
		}
	}
}
