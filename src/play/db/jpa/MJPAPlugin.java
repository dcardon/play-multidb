package play.db.jpa;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.Cache;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.hibernate.CallbackException;
import org.hibernate.EmptyInterceptor;
import org.hibernate.collection.PersistentCollection;
import org.hibernate.ejb.Ejb3Configuration;
import org.hibernate.type.Type;

import play.PlayPlugin;
import play.Logger;
import play.Play;
import play.db.MDB;
import play.exceptions.JPAException;
import play.mvc.Http.Request;
import play.mvc.results.NotFound;
import play.utils.Utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * The multiple JPA plugin, which supports setting the database connection based on a request parameter. 
 * 
 * @author dcardon
 */
public class MJPAPlugin extends PlayPlugin
{
	public static Log log = LogFactory.getLog(MJPAPlugin.class);

	public static boolean autoTxs = true;

	/**
	 * The map from database keys to their corresponding entity manager factories.
	 */
	public static Map<String, EntityManagerFactory> factoryMap = new HashMap<String, EntityManagerFactory>();

	/**
	 * The default key extractor, if none is defined within the application classes.
	 */
	public static RequestDBKeyExtractor keyExtractor = new DomainDBKeyExtractor();

	@Override
	public void beforeInvocation()
	{
		//
		// Prevent the regular JPA Plugin from starting a transaction.
		//
		JPA.entityManagerFactory = null;
		
		//
		//	If we have no databases defined, and we have a directive to permit this state, allow it.
		//
		if (factoryMap.isEmpty() && Play.configuration.getProperty("mjpa.runWithNoDB","").equals("true"))
		{
			log.debug("Empty factory map--using dummy factory");
			JPA.entityManagerFactory = getDummyFactory();
			return;
		}
		
		log.debug("Extracting DB key from request: " + Request.current());

		//
		// Find the database key, so that we'll have one for the transaction.
		//
		String dbKey = keyExtractor.extractKey(Request.current());
		log.debug("Found key: " + dbKey);
		try
		{
			if (dbKey != null)
			{
    			//
    			// Start the transaction
    			//
    			startTx(dbKey, false);
			}
		}
		catch (InvalidDatabaseException e)
		{
			throw new NotFound(e.getMessage());
		}
	}

	/**
	 * Starts the transaction on the specified database.
	 * 
	 * @param factory
	 * @param readOnly
	 */
	public static void startTx(String dbKey, boolean readOnly)
	{
		EntityManagerFactory factory = factoryMap.get(dbKey);
		log.debug("Starting transaction with factory" + factory + " on DB: " + dbKey);
		if (dbKey == null || factory == null)
		{	
			log.warn("No database found for key: '" + dbKey + "'.  Skipping database connection.");
			return;
		}

		EntityManager manager = factory.createEntityManager();
		manager.setFlushMode(FlushModeType.COMMIT);
		if (autoTxs)
		{
			manager.getTransaction().begin();
		}
		log.debug("Creating JPA context: " + manager + " for db: " + dbKey);
		JPA.createContext(manager, readOnly);
	}

	/**
	 * Rolls back the transaction in the current JPA's local implementation.
	 * 
	 * @param rollback
	 */
	public static void closeTx(boolean rollback)
	{
		if (JPA.local.get() == null || JPA.get().entityManager == null)
		{
			return;
		}
		EntityManager manager = JPA.get().entityManager;
		try
		{
			if (autoTxs)
			{
				if (manager.getTransaction().isActive())
				{
					if (JPA.get().readonly || rollback
							|| manager.getTransaction().getRollbackOnly())
					{
						manager.getTransaction().rollback();
					}
					else
					{
						try
						{
							if (autoTxs)
							{
								manager.getTransaction().commit();
							}
						}
						catch (Throwable e)
						{
							for (int i = 0; i < 10; i++)
							{
								if (e instanceof PersistenceException
										&& e.getCause() != null)
								{
									e = e.getCause();
									break;
								}
								e = e.getCause();
								if (e == null)
								{
									break;
								}
							}
							throw new JPAException("Cannot commit", e);
						}
					}
				}
			}
		}
		finally
		{
			manager.close();
			JPA.clearContext();
		}
	}

	@Override
	public void afterInvocation()
	{
		closeTx(false);
	}

	@Override
	public void afterActionInvocation()
	{
		closeTx(false);
	}

	@Override
	public void onInvocationException(Throwable e)
	{
		closeTx(true);
	}

	@Override
	public void invocationFinally()
	{
		closeTx(true);
	}

	/**
	 * Retrieves the default dialect for this plugin
	 * 
	 * @param driver
	 * @return
	 */
	static String getDefaultDialect(String driver)
	{
		//
		//	Use the explicit dialect if it's listed, otherwise go with the implicit.
		//
		String dialect = Play.configuration.getProperty("mjpa.dialect");
		if (dialect != null)
		{
			return dialect;
		}
		
		if (driver.equals("org.hsqldb.jdbcDriver"))
		{
			return "org.hibernate.dialect.HSQLDialect";
		}
		else if (driver.equals("com.mysql.jdbc.Driver"))
		{
			return "play.db.jpa.MySQLDialect";
		}
		else
		{
			throw new UnsupportedOperationException(
					"I do not know which hibernate dialect to use with " + driver
							+ ", use the property jpa.dialect in config file");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onApplicationStart()
	{
		//
		//	NOTE: this uses the JPA class to store the request's entityManagerFactory.
		//	The trick is that the MJPAPlugin has higher priority than Play's native JPAPlugin.
		//
		if (JPA.entityManagerFactory == null)
		{
			List<Class> classes = Play.classloader.getAnnotatedClasses(Entity.class);
			if (classes.isEmpty()
					&& Play.configuration.getProperty("jpa.entities", "").equals(""))
			{
				return;
			}
			if (MDB.datasources == null || MDB.datasources.isEmpty())
			{
                if (Play.configuration.getProperty("mjpa.runWithNoDB","").equals("true"))
                {
                	//
                	//	Create a dummy entity manager factory, so that JPA is prevented from screaming.
                	//
                	JPA.entityManagerFactory = getDummyFactory();
                	log.info("No properly configured databases found.  JPA will not be initialized until databases are added");
                }
                else
                {
                    throw new JPAException("Cannot start a MJPA manager without a properly configured database", new NullPointerException("No datasource configured"));
                }
			}
			else
			{
    			//
    			//	Iterate over the datasources and build a configuration for each.
    			//
    			for (Entry<String, DataSource> entry : MDB.datasources.entrySet())
    			{
    				ComboPooledDataSource datasource = (ComboPooledDataSource) entry.getValue();
    				
    				Ejb3Configuration cfg = buildEjbConfiguration(classes, datasource);
    				Logger.trace("Initializing JPA ...");
    				try
    				{
    					EntityManagerFactory factory = cfg.buildEntityManagerFactory(); 
    					JPA.entityManagerFactory = factory;
    					factoryMap.put(entry.getKey(), factory);
    					log.debug("Added datasource: " + datasource.getJdbcUrl());
    				}
    				catch (PersistenceException e)
    				{
    					throw new JPAException(e.getMessage(), e.getCause() != null
    							? e.getCause() : e);
    				}
    			}
			}
			JPQL.instance = new JPQL();
		}
		
		//
		//	Set up the key extractor here, by looking for an application class that implements it.
		//
		List<Class> extractors = Play.classloader.getAssignableClasses(RequestDBKeyExtractor.class);
		if (extractors.size() > 1)
		{
			throw new JPAException("Too many DB Key extract classes.  " +
					"The Multiple DB plugin must use a single extractor class to " +
					"specify its extractor.  These classes where found: " + extractors);
		}
		else if (!extractors.isEmpty())
		{
			Class clazz = extractors.get(0);
			try
			{
				keyExtractor = (RequestDBKeyExtractor) clazz.newInstance();
			}
			catch (InstantiationException e)
			{
				log.error("Unable to instantiate extractor class:",e);
			}
			catch (IllegalAccessException e)
			{
				log.error("Invalid access to extractor class:",e);
			}
			log.debug("Using application DB key extractor class: " + keyExtractor.getClass().getName());
		}
		else
		{
			log.debug("Using default DB key extractor class: " + keyExtractor.getClass().getName());
		}
	}

	/**
	 * @param classes
	 * @param datasource
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Ejb3Configuration buildEjbConfiguration(List<Class> classes,
			ComboPooledDataSource datasource)
	{
		Ejb3Configuration cfg = new Ejb3Configuration();
		cfg.setDataSource(datasource);
		if (!Play.configuration.getProperty("jpa.ddl", "update").equals("none"))
		{
			cfg.setProperty("hibernate.hbm2ddl.auto", Play.configuration.getProperty(
					"jpa.ddl", "update"));
		}
		cfg.setProperty("hibernate.dialect", getDefaultDialect(datasource.getDriverClass()));
		cfg.setProperty("javax.persistence.transaction", "RESOURCE_LOCAL");

		// Explicit SAVE for JPASupport is implemented here
		// ~~~~~~
		// We've hacked the org.hibernate.event.def.AbstractFlushingEventListener line
		// 271, to flush collection update,remove,recreation
		// only if the owner will be saved.
		// As is:
		// if (session.getInterceptor().onCollectionUpdate(coll, ce.getLoadedKey())) {
		// actionQueue.addAction(...);
		// }
		//
		// This is really hacky. We should move to something better than Hibernate
		// like EBEAN
		cfg.setInterceptor(new EmptyInterceptor()
		{
			/**
			 * Generated GUID for serialization support.
			 */
			private static final long serialVersionUID = -8670026536584880961L;

			@Override
			public int[] findDirty(Object o, Serializable id, Object[] arg2,
					Object[] arg3, String[] arg4, Type[] arg5)
			{
				if (o instanceof JPABase && !((JPABase) o).willBeSaved)
				{
					return new int[0];
				}
				return null;
			}

			@Override
			public boolean onCollectionUpdate(Object collection, Serializable key)
					throws CallbackException
			{
				if (collection instanceof PersistentCollection)
				{
					Object o = ((PersistentCollection) collection).getOwner();
					if (o instanceof JPABase)
					{
						return ((JPABase) o).willBeSaved;
					}
				}
				else
				{
					System.out.println("HOO: Case not handled !!!");
				}
				return super.onCollectionUpdate(collection, key);
			}

			@Override
			public boolean onCollectionRecreate(Object collection, Serializable key)
					throws CallbackException
			{
				if (collection instanceof PersistentCollection)
				{
					Object o = ((PersistentCollection) collection).getOwner();
					if (o instanceof JPABase)
					{
						return ((JPABase) o).willBeSaved;
					}
				}
				else
				{
					System.out.println("HOO: Case not handled !!!");
				}
				return super.onCollectionRecreate(collection, key);
			}

			@Override
			public boolean onCollectionRemove(Object collection, Serializable key)
					throws CallbackException
			{
				if (collection instanceof PersistentCollection)
				{
					Object o = ((PersistentCollection) collection).getOwner();
					if (o instanceof JPABase)
					{
						return ((JPABase) o).willBeSaved;
					}
				}
				else
				{
					System.out.println("HOO: Case not handled !!!");
				}
				return super.onCollectionRemove(collection, key);
			}
		});
		if (Play.configuration.getProperty("jpa.debugSQL", "false").equals("true"))
		{
			org.apache.log4j.Logger.getLogger("org.hibernate.SQL").setLevel(Level.ALL);
		}
		else
		{
			org.apache.log4j.Logger.getLogger("org.hibernate.SQL").setLevel(Level.OFF);
		}
		// inject additional hibernate.* settings declared in Play! configuration
		cfg.addProperties((Properties) Utils.Maps.filterMap(Play.configuration,
				"^hibernate\\..*"));

		try
		{
			Field field = cfg.getClass().getDeclaredField("overridenClassLoader");
			field.setAccessible(true);
			field.set(cfg, Play.classloader);
		}
		catch (Exception e)
		{
			Logger.error(e,
					"Error trying to override the hibernate classLoader (new hibernate version ???)");
		}
		for (Class<? extends Annotation> clazz : classes)
		{
			if (clazz.isAnnotationPresent(Entity.class))
			{
				cfg.addAnnotatedClass(clazz);
				Logger.trace("JPA Model : %s", clazz);
			}
		}
		String[] moreEntities = Play.configuration.getProperty("jpa.entities", "").split(
				", ");
		for (String entity : moreEntities)
		{
			if (entity.trim().equals(""))
				continue;
			try
			{
				if (Play.classes.getApplicationClass(entity) == null)
				{
					cfg.addAnnotatedClass(Play.classloader.loadClass(entity));
				}
				else
				{
					cfg.addAnnotatedClass(Play.classes.getApplicationClass(entity).javaClass);
				}
				
			}
			catch (Exception e)
			{
				Logger.warn("JPA -> Entity not found: %s", entity);
			}
		}
		return cfg;
	}
	
	private static EntityManagerFactory getDummyFactory()
	{
		return new EntityManagerFactory() {

			@Override
			public void close()
			{
			}

			@Override
			public EntityManager createEntityManager()
			{
				return getDummyManager();
			}

			@SuppressWarnings("unchecked")
			@Override
			public EntityManager createEntityManager(Map arg0)
			{
				return getDummyManager();
			}

			@Override
			public boolean isOpen()
			{
				return false;
			}
			
			@Override
			public Cache getCache() {
				return null;
			}
			
			@Override
			public CriteriaBuilder getCriteriaBuilder() {
				return null;
			}
			
			@Override
			public Metamodel getMetamodel() {
				return null;
			}
			
			@Override
			public PersistenceUnitUtil getPersistenceUnitUtil() {
				return null;
			}
			
			@Override
			public Map<String, Object> getProperties() {
				return null;
			}
		};
	}
	
	private static EntityManager getDummyManager()
	{
		return new EntityManager()
		{
			@Override
			public void clear()
			{
			}

			@Override
			public void close()
			{
			}

			@Override
			public boolean contains(Object arg0)
			{
				return false;
			}

			@Override
			public Query createNamedQuery(String arg0)
			{
				return null;
			}

			@Override
			public Query createNativeQuery(String arg0)
			{
				return null;
			}

			@SuppressWarnings("unchecked")
			@Override
			public Query createNativeQuery(String arg0, Class arg1)
			{
				return null;
			}

			@Override
			public Query createNativeQuery(String arg0, String arg1)
			{
				return null;
			}

			@Override
			public Query createQuery(String arg0)
			{
				return null;
			}

			@Override
			public <T> T find(Class<T> arg0, Object arg1)
			{
				return null;
			}

			@Override
			public void flush()
			{
			}

			@Override
			public Object getDelegate()
			{
				return null;
			}

			@Override
			public FlushModeType getFlushMode()
			{
				return null;
			}

			@Override
			public <T> T getReference(Class<T> arg0, Object arg1)
			{
				return null;
			}

			@Override
			public EntityTransaction getTransaction()
			{
				return new EntityTransaction()
				{
					@Override
					public void begin()
					{
					}

					@Override
					public void commit()
					{
					}

					@Override
					public boolean getRollbackOnly()
					{
						return false;
					}

					@Override
					public boolean isActive()
					{
						return false;
					}

					@Override
					public void rollback()
					{
					}

					@Override
					public void setRollbackOnly()
					{
					}
				};
			}

			@Override
			public boolean isOpen()
			{
				return false;
			}

			@Override
			public void joinTransaction()
			{
			}

			@Override
			public void lock(Object arg0, LockModeType arg1)
			{
			}

			@Override
			public <T> T merge(T arg0)
			{
				return null;
			}

			@Override
			public void persist(Object arg0)
			{
			}

			@Override
			public void refresh(Object arg0)
			{
			}

			@Override
			public void remove(Object arg0)
			{
			}

			@Override
			public void setFlushMode(FlushModeType arg0)
			{
			}

			@Override
			public <T> TypedQuery<T> createNamedQuery(String arg0, Class<T> arg1) {
				return null;
			}

			@Override
			public <T> TypedQuery<T> createQuery(CriteriaQuery<T> arg0) {
				return null;
			}

			@Override
			public <T> TypedQuery<T> createQuery(String arg0, Class<T> arg1) {
				return null;
			}

			@Override
			public void detach(Object arg0) {
			}

			@Override
			public <T> T find(Class<T> arg0, Object arg1, Map<String, Object> arg2) {
				return null;
			}

			@Override
			public <T> T find(Class<T> arg0, Object arg1, LockModeType arg2) {
				return null;
			}

			@Override
			public <T> T find(Class<T> arg0, Object arg1, LockModeType arg2,
			 Map<String, Object> arg3) {
				return null;
			}

			@Override
			public CriteriaBuilder getCriteriaBuilder() {
				return null;
			}

			@Override
			public EntityManagerFactory getEntityManagerFactory() {
				return null;
			}

			@Override
			public LockModeType getLockMode(Object arg0) {
				return null;
			}

			@Override
			public Metamodel getMetamodel() {
				return null;
			}

			@Override
			public Map<String, Object> getProperties() {
				return null;
			}

			@Override
			public void lock(Object arg0, LockModeType arg1, Map<String, Object> arg2) {
			}

			@Override
			public void refresh(Object arg0, Map<String, Object> arg1) {
			}

			@Override
			public void refresh(Object arg0, LockModeType arg1) {
			}

			@Override
			public void refresh(Object arg0, LockModeType arg1,
			 Map<String, Object> arg2) {
			}

			@Override
			public void setProperty(String arg0, Object arg1) {
			}

			@Override
			public <T> T unwrap(Class<T> arg0) {
				return null;
			}
			
		};
	}
}
