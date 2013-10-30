//
// Copyright (c) 1998,2007 Michael Toth
// Spiralcraft Inc., All Rights Reserved
//
// This package is part of the Spiralcraft project and is licensed under
// a multiple-license framework.
//
// You may not use this file except in compliance with the terms found in the
// SPIRALCRAFT-LICENSE.txt file at the top of this distribution, or available
// at http://www.spiralcraft.org/licensing/SPIRALCRAFT-LICENSE.txt.
//
// Unless otherwise agreed to in writing, this software is distributed on an
// "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
//
package spiralcraft.sql.data.store;


import spiralcraft.lang.BindException;
import spiralcraft.lang.Binding;
import spiralcraft.lang.Focus;
import spiralcraft.log.Level;

import spiralcraft.data.DataConsumer;
import spiralcraft.data.access.Entity;
import spiralcraft.data.access.Snapshot;
import spiralcraft.data.access.kit.AbstractStore;
import spiralcraft.data.access.kit.AbstractStoreSequence;
import spiralcraft.data.access.kit.EntityBinding;

import spiralcraft.data.Aggregate;
import spiralcraft.data.DataException;
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.Field;
import spiralcraft.data.Sequence;
import spiralcraft.data.Type;
import spiralcraft.data.Tuple;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Queryable;

import spiralcraft.data.spi.EditableArrayListAggregate;
import spiralcraft.data.spi.EditableArrayTuple;
import spiralcraft.data.transaction.Transaction;
import spiralcraft.data.types.standard.AnyType;

import spiralcraft.sql.Dialect;

import spiralcraft.sql.ddl.DDLStatement;
import spiralcraft.sql.pool.ConnectionFactory;
import spiralcraft.sql.pool.ConnectionPool;
import spiralcraft.util.Path;
import spiralcraft.vfs.Container;
import spiralcraft.vfs.Resolver;
import spiralcraft.vfs.Resource;
import spiralcraft.vfs.Session;
import spiralcraft.vfs.file.FileResource;
import spiralcraft.vfs.jar.JarFileResource;


import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.CommonDataSource;
import javax.sql.XAConnection;

import java.util.List;

import spiralcraft.common.ContextualException;
import spiralcraft.common.LifecycleException;


/**
 * A Store backed by a SQL database
 *
 */
public class SqlStore
  extends AbstractStore
{
  
  private CommonDataSource dataSource;
  private Binding<CommonDataSource> dataSourceX;
  private ConnectionPool<SqlStoreConnection> connectionPool
    =new ConnectionPool<SqlStoreConnection>();
  
  { 
    connectionPool.setConnectionFactory
      (new ConnectionFactory<SqlStoreConnection>()
        {
          @Override
          public SqlStoreConnection newConnection(Connection delegate)
            throws SQLException
          { 
            log.fine("Creating connection from "+delegate);
            return new SqlStoreConnection(delegate);
          }
          
          @Override
          public SqlStoreConnection newConnection(Connection delegate,XAConnection xa)
            throws SQLException
          { 
            log.fine("Creating XA connection from "+delegate);
            return new SqlStoreConnection(delegate,xa);
          }          
        }
      );
  }
  private TypeManager typeManager=new TypeManager();
  private SqlResourceManager resourceManager
    =new SqlResourceManager(this);
  
  private TableMapping sequenceTableMapping;
  
  private URI localResourceURI;
  
  private Dialect dialect;
  private boolean autoUpgrade;
  private Level statementLogLevel=Level.INFO;
  
  public SqlStore()
    throws DataException
  { 
  }
  
  /**
   * Specify the dataSource that will supply JDBC connections 
   */
  public void setDataSource(CommonDataSource dataSource)
  { this.dataSource=dataSource;
  }
  
  public void setDataSourceX(Binding<CommonDataSource> dataSourceX)
  { this.dataSourceX=dataSourceX;
  }
  
  public ConnectionPool<SqlStoreConnection> getConnectionPool()
  { return connectionPool;
  }
  
  /**
   * Specify the Dialect for the specific database server product that will provide
   *   product specific information and type translations.
   */
  public void setDialect(Dialect dialect)
  { this.dialect=dialect;
  }

  @Override
  public void setLocalResourceURI(URI localResourceURI)
  { this.localResourceURI=localResourceURI;
  }

  
  /**
   * Obtain a direct reference to the DataSource that supplies JDBC connections
   */
  public CommonDataSource getDataSource()
  { return dataSource;
  }

  public void setAutoUpgrade(boolean autoUpgrade)
  { this.autoUpgrade=autoUpgrade;
  }
  
  /**
   * Obtain a direct reference to the TypeManager that maps 
   *   metadata objects to SQL objects
   */
  public TypeManager getTypeManager()
  { return typeManager;
  }
  

  private void resolve()
    throws ContextualException
  { 

    
    sequenceTableMapping
      =new TableMapping();
    sequenceTableMapping.setType(sequenceType);
    sequenceTableMapping.setTableName("Sequence");
    typeManager.addTableMapping(sequenceTableMapping);
    
    typeManager.setStore(this);
    if (schema!=null)
    { 
      typeManager.setSchemaMappings
        (new SchemaMapping[] {new SchemaMapping(null,schema)});
    }
    if (dialect!=null)
    { typeManager.setDialect(dialect);
    }
    typeManager.setAutoUpgrade(autoUpgrade);
    typeManager.resolve();

  }
  
  
  
  /**
   * <p>Check out a connection from the pool or the data source.
   * </p>
   * 
   * @return
   * @throws SQLException
   */
  public Connection checkoutConnection()
    throws SQLException
  { 
    try
    { return connectionPool.checkout();
    }
    catch (InterruptedException x)
    { throw new SQLException("Timed out waiting for connection pool");
    }
  }
  
  @Override
  public Focus<?> bind(Focus<?> focus)
    throws ContextualException
  {
    super.preBind(focus);
    try
    { resolve();
    }
    catch (DataException x)
    { throw new BindException("Error resolving data model",x);
    }
    
    TableMapping[] mappings=typeManager.getTableMappings();
    for (TableMapping mapping: mappings) 
    { 
      EntityBinding binding=createEntityBinding(new Entity(mapping.getType()));
      binding.setAuthoritative(true);
      binding.setAccessor(mapping);
      binding.setUpdater(mapping.getUpdater());   
      addEntityBinding(binding);
      
    }
    
    
    
    focus=super.bind(focus);
    if (dataSourceX!=null)
    { dataSourceX.bind(focus);
    }
    return focus;
  }

  
  @Override
  public void start()
    throws LifecycleException
  { 
    if (dataSourceX!=null)
    { setDataSource(dataSourceX.get());
    }
    log.info("Serving SQL data from "+this.getLocalResourceURI());
    if (dataSource!=null)
    { 
      connectionPool.setDataSource(dataSource);
      connectionPool.setDialect(dialect);
      connectionPool.start();
    }
    try
    { onAttach(); // XXX Waiting for auto-recovery implementation
    }
    catch (DataException x)
    { 
      connectionPool.stop();
      throw new LifecycleException(x.toString(),x);
    }
    super.start();
  }

  @Override
  public void stop()
    throws LifecycleException
  {
    log.info
      ("Stopping SqlStore "+(getName()!=null?getName():"")+": "
      +(this.getLocalResourceURI()!=null?this.getLocalResourceURI():"")
      );
    super.stop();
    connectionPool.stop();
    log.info
      ("Stopped SqlStore "+(getName()!=null?getName():"")+": "
      +(this.getLocalResourceURI()!=null?this.getLocalResourceURI():"")
      );
  }
  
  public void setStatementLogLevel(Level level)
  { this.statementLogLevel=level;
  }
    
  public Level getStatementLogLevel()
  { return statementLogLevel;
  }

  public void executeDDL(List<DDLStatement> statements)
    throws DataException
  {
    Transaction tx
      =Transaction.startContextTransaction(Transaction.Nesting.PROPOGATE);
    tx.setDebug(debugLevel.isDebug());
    SqlBranch branch=resourceManager.branch(tx);
    try
    {
      Connection conn=null;
      boolean autoCommit=false;
      Statement sqlStatement=null;
      
      try
      { 
        
        conn=getContextConnection();
        log.fine("Connection: "+conn);
        autoCommit=conn.getAutoCommit();
        if (!branch.is2PC())
        { conn.setAutoCommit(false);
        }
        sqlStatement=conn.createStatement();
        
        for (DDLStatement statement: statements)
        {
          StringBuilder buff=new StringBuilder();
          statement.write(buff,"", null);
          try
          {
            sqlStatement.executeUpdate(buff.toString());
            log.fine("Executed "+buff.toString());
          }
          catch (SQLException x)
          { throw new SQLException("Failed to execute DDL: "+buff,x);
          }
        }
        sqlStatement.close();
        if (!branch.is2PC())
        {
          conn.commit();
          conn.setAutoCommit(autoCommit);
          sqlStatement.close();
          conn.close();
        }
        tx.commit();
      }
      catch (SQLException x)
      { 
        if (conn!=null && !branch.is2PC())
        { 
          try
          {
            conn.rollback();
            conn.setAutoCommit(autoCommit);
            if (sqlStatement!=null)
            { sqlStatement.close();
            }
            conn.close();
          }
          catch (SQLException y)
          { throw new DataException("Error '"+y+"' recovering from error '"+x+"'",y);
          }
        }
        throw new DataException("Error running DDL "+x,x);
      }
    }
    finally
    { tx.complete();
    }
  }
  
  
  /**
   * @return A DataConsumer which is used to push one or more updates into
   *   this Store. 
   */
  @Override 
  public DataConsumer<DeltaTuple> getUpdater(Type<?> type)
    throws DataException
  { return assertTableMapping(type).getUpdater();
  }
    
  
  /**
   * Return the Connection that is coordinated with the Transaction in-context,
   *   if any.
   */
  public Connection getContextConnection()
    throws DataException
  { 
    Transaction transaction=Transaction.getContextTransaction();
    if (transaction!=null)
    { return resourceManager.branch(transaction).getConnection();
    }
    else
    { 
      try
      { return checkoutConnection();
      }
      catch (SQLException x)
      { throw new DataException("Error getting connection",x);
      }
    }
  }

  @Override
  protected Sequence createSequence(Field<?> field)
  { return new SqlSequence(field.getURI());
  }
  
  class SqlSequence
    extends AbstractStoreSequence
  {
    
    public SqlSequence (URI uri)
    { super(SqlStore.this,uri,sequenceTableMapping);
    }

    @Override
    protected BoundQuery<?,Tuple> bindQuery(Query sequenceQuery)
      throws DataException
    { 
      BoundQuery<?,Tuple> boundQuery
        =sequenceTableMapping.query(sequenceQuery,uriFocus);
      if (debugLevel.isDebug())
      { 
        log.fine("Bound sequence query = "+boundQuery);
        boundQuery.setDebugLevel(Level.FINE);
      }
      return boundQuery;
    }
    
    @Override
    public void updateInTx(DeltaTuple dt)
      throws DataException
    {
      DataConsumer<DeltaTuple> updater
        =getUpdater(sequenceType);
      updater.dataInitialize(sequenceType.getFieldSet());
            
      updater.dataAvailable(dt);
      updater.dataFinalize();
    }

    @Override
    public void insertInTx(DeltaTuple dt)
      throws DataException
    {
      DataConsumer<DeltaTuple> updater
        =getUpdater(sequenceType);
      updater.dataInitialize(sequenceType.getFieldSet());
            
      updater.dataAvailable(dt);
      updater.dataFinalize();
    }
    
  }
  
  /**
   * Called after the database becomes available.
   */
  private void onAttach()
    throws DataException
  { 
    typeManager.updateMetaData();
    typeManager.ensureDataVersion();
    
  }

  private TableMapping assertTableMapping(Type<?> type)
    throws DataException
  {    
    TableMapping mapping=typeManager.getTableMapping(type);
    if (mapping==null)
    { throw new DataException("SqlStore: Not a locally handled Type "+type.getURI());
    }
    return mapping;
  }

  @Override
  public void update(Snapshot snapshot)
  {
    for (Aggregate<Tuple> aggregate : snapshot.getData())
    { 
      Type<?> type=aggregate.getType().getContentType();
      Queryable<?> queryable=getQueryable(type);
      if (queryable==null || !(queryable instanceof TableMapping))
      { log.warning("Ignoring snapshot of "+type.getURI());
      }
      else
      { 
        try
        { ((TableMapping) queryable).update(aggregate);
        }
        catch (DataException x)
        { 
          log.log
            (Level.WARNING,"Failed to deliver subscription to "+type.getURI()
            ,x);
        }
      }
      
    }
    lastTransactionId=snapshot.getTransactionId();
  }

  @Override
  public Snapshot snapshot(long transactionId)
    throws DataException
  {
    if (transactionId==0 || lastTransactionId>transactionId)
    {
      EditableArrayTuple snapshot=new EditableArrayTuple(Snapshot.TYPE);
      
      // Note- do not set to 0 here or entire dataset will be sent repeatedly
      snapshot.set("transactionId",(lastTransactionId!=0)?lastTransactionId:1);
      
      EditableArrayListAggregate<Aggregate<Tuple>> data
        =new EditableArrayListAggregate<Aggregate<Tuple>>
          (Type.resolve(AnyType.TYPE_URI+".list.list")
          );
      for (Queryable<?> queryable:getPrimaryQueryables())
      {
        TableMapping table
          =(TableMapping) queryable;
        if (transactionId==0
            || table.getLastTransactionId()>transactionId
            )
        { data.add(table.snapshot());
        }
      }
      snapshot.set("data",data);
      return Snapshot.TYPE.fromData(snapshot,null);
    }
    else
    { return null;
    }
  }

  /**
   * Restore data from a backup. Replaces all data with the backup copy stored
   *   in the specified URI.
   * 
   * @param uri
   */
  public void restore(URI uri)
    throws IOException,DataException
  { 

    Session session=new Session();
    try
    {
      Resource resource=Resolver.getInstance().resolve(uri);
      Container container;
      if (resource.getLocalName().endsWith(".zip") 
          || resource.getLocalName().endsWith(".jar")
         )
      {
        FileResource localResource=session.asLocalResource(resource);
        JarFileResource jarResource
          =new JarFileResource(localResource.getFile(),Path.ROOT_PATH);
        container=jarResource.asContainer();
      }
      else
      { container=resource.asContainer();
      }
      if (container==null)
      { throw new IOException("Not a container "+resource.getURI());
      }

      Transaction tx
        =Transaction.startContextTransaction(Transaction.Nesting.PROPOGATE);
      tx.setDebug(debugLevel.isDebug());
      try
      {
        for (TableMapping mapping: typeManager.getTableMappings())
        {
          if (mapping.getTableName()!=null)
          { 
            Resource xmlData
              =container.getChild(mapping.getTableName()+".data.xml");
            if (xmlData.exists())
            { log.fine(xmlData.getURI()+" exists");
            }
            else 
            { 
              log.fine(xmlData.getURI()+" not found");            
              xmlData=container.getChild(mapping.getTableName()+".xml");
              if (xmlData.exists())
              { log.fine(xmlData.getURI()+" exists");
              }
              else
              { log.fine(xmlData.getURI()+" not found");
              }
            }
            
            if (xmlData.exists())
            { mapping.restore(xmlData);
            }
          }
        }
        tx.commit();
      }
      finally
      { tx.complete();
      }
    }
    finally
    { session.release();
    }
  }
  
  @Override
  public URI getLocalResourceURI()
  { return localResourceURI;
  }

  @Override
  protected Sequence createTxIdSequence()
  {
    return new SqlSequence
      (URI.create("class:/spiralcraft/sql/store/SqlStore.txId"));
  }

  
}
