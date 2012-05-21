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
import spiralcraft.lang.Focus;
import spiralcraft.lang.SimpleFocus;
import spiralcraft.lang.spi.SimpleChannel;
import spiralcraft.log.Level;

import spiralcraft.data.DataConsumer;
import spiralcraft.data.access.Entity;
import spiralcraft.data.access.SerialCursor;
import spiralcraft.data.access.Snapshot;
import spiralcraft.data.access.kit.AbstractStore;
import spiralcraft.data.access.kit.EntityBinding;

import spiralcraft.data.Aggregate;
import spiralcraft.data.DataException;
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.EditableTuple;
import spiralcraft.data.Field;
import spiralcraft.data.Sequence;
import spiralcraft.data.Type;
import spiralcraft.data.Tuple;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Queryable;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.Scan;

import spiralcraft.data.spi.ArrayDeltaTuple;
import spiralcraft.data.spi.EditableArrayListAggregate;
import spiralcraft.data.spi.EditableArrayTuple;
import spiralcraft.data.transaction.Transaction;
import spiralcraft.data.transaction.WorkException;
import spiralcraft.data.transaction.WorkUnit;
import spiralcraft.data.transaction.Transaction.Nesting;
import spiralcraft.data.types.standard.AnyType;

import spiralcraft.sql.data.query.BoundSelection;
import spiralcraft.sql.data.query.BoundScan;

import spiralcraft.sql.ddl.DDLStatement;
import spiralcraft.sql.pool.ConnectionFactory;
import spiralcraft.sql.pool.ConnectionPool;


import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

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
  
  private DataSource dataSource;
  private ConnectionPool<SqlStoreConnection> connectionPool
    =new ConnectionPool<SqlStoreConnection>();
  
  { 
    connectionPool.setConnectionFactory
      (new ConnectionFactory<SqlStoreConnection>()
        {
          @Override
          public SqlStoreConnection newConnection(Connection delegate)
          { return new SqlStoreConnection(delegate);
          }
        }
      );
  }
  private TypeManager typeManager=new TypeManager();
  private SqlResourceManager resourceManager
    =new SqlResourceManager(this);
  
  private TableMapping sequenceTableMapping;
  
  @SuppressWarnings("unused")
  private URI localResourceURI;
  
  public SqlStore()
    throws DataException
  {
  }
  
  /**
   * Specify the dataSource that will supply JDBC connections 
   */
  public void setDataSource(DataSource dataSource)
  { this.dataSource=dataSource;
  }
  
  public ConnectionPool<SqlStoreConnection> getConnectionPool()
  { return connectionPool;
  }

  @Override
  public void setLocalResourceURI(URI localResourceURI)
  { this.localResourceURI=localResourceURI;
  }
  
  /**
   * Obtain a direct reference to the DataSource that supplies JDBC connections
   */
  public DataSource getDataSource()
  { return dataSource;
  }

  
  /**
   * Obtain a direct reference to the TypeManager that maps 
   *   metadata objects to SQL objects
   */
  public TypeManager getTypeManager()
  { return typeManager;
  }

  public void setTypeManager(TypeManager typeManager)
  { this.typeManager=typeManager;
  }
  

  private void resolve()
    throws DataException
  { 

    
    sequenceTableMapping
      =new TableMapping();
    sequenceTableMapping.setType(sequenceType);
    sequenceTableMapping.setTableName("Sequence");
    typeManager.addTableMapping(sequenceTableMapping);
    
    typeManager.setStore(this);
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
    
    
    
    return super.bind(focus);
  }

  
  @Override
  public void start()
    throws LifecycleException
  { 
    connectionPool.setDataSource(dataSource);
    connectionPool.start();
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
    super.stop();
    connectionPool.stop();
    // log.fine("Stopped SqlStore");
  }
  
  public void executeDDL(List<DDLStatement> statements)
    throws DataException
  {
    Connection conn=null;
    boolean autoCommit=false;
    Statement sqlStatement=null;
    
    try
    { 
      
      conn=getContextConnection();
      autoCommit=conn.getAutoCommit();
      conn.setAutoCommit(false);
      sqlStatement=conn.createStatement();
      
      for (DDLStatement statement: statements)
      {
        StringBuilder buff=new StringBuilder();
        statement.write(buff,"", null);
        sqlStatement.execute(buff.toString());
      }
      conn.commit();
      conn.setAutoCommit(autoCommit);
      sqlStatement.close();
      conn.close();
    }
    catch (SQLException x)
    { 
      if (conn!=null)
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
  

  
  @Override
  public BoundQuery<?,Tuple> query(Query query,Focus<?> focus)
    throws DataException
  { 
    if (query instanceof Selection)
    { 
      BoundSelection boundSelection
        =new BoundSelection((Selection) query,focus,this);
      System.err.println("SqlStore.query: remainder="+boundSelection.getRemainderCriteria());
      return boundSelection;
    }
    else if (query instanceof Scan)
    { return new BoundScan((Scan) query,focus,this);
    }
    else
    { return super.query(query,focus);
    }
  }
  
  
  /**
   * @return A DataConsumer which is used to push one or more updates into
   *   this Store. 
   */
  @Override
  public DataConsumer<DeltaTuple> getUpdater(Type<?> type,Focus<?> focus)
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
    implements Sequence
  {

    private int increment;
    private volatile long next;
    private volatile long stop;
    private BoundQuery<?,Tuple> boundQuery;
    private Focus<URI> uriFocus;
    private URI uri;
    
    public SqlSequence (URI uri)
    { 
      this.uri=uri;
      uriFocus=new SimpleFocus<URI>(new SimpleChannel<URI>(uri,true));
    }

    @Override
    public void start()
      throws LifecycleException
    {
      try
      {
        boundQuery
          =sequenceTableMapping.query(sequenceQuery,uriFocus);
      }
      catch (DataException x)
      { throw new LifecycleException("Error binding sequence query");
      }
    }
    
    @Override
    public void stop()
    {
    }
    
    public void allocate()
      throws DataException
    {
      synchronized(sequenceTableMapping)
      {
        SerialCursor<Tuple> result=boundQuery.execute();
        Tuple resultRow=null;
        try
        { 
          resultRow=result.next()?result.getTuple().snapshot():null;
          if (result.next())
          {
            throw new DataException
              ("Cardinality violation in Sequence store- non unique URI "+uri); 
          }
        }
        finally
        { result.close();
        }
        
        if (resultRow==null)
        {
          final EditableTuple row=new EditableArrayTuple(sequenceType.getScheme());
          row.set("uri",uri);
          row.set("nextValue",200);
          row.set("increment",100);
          next=100;
          stop=200;
          increment=100;
            
          new WorkUnit<Void>()
          {
            { this.nesting=Nesting.ISOLATE;
            }
            
            @Override
            protected Void run() throws WorkException
            {
              try
              {
                DataConsumer<DeltaTuple> updater
                  =getUpdater(sequenceType,null);
                updater.dataInitialize(sequenceType.getFieldSet());
            
                updater.dataAvailable(new ArrayDeltaTuple(null,row));
                updater.dataFinalize();
              
                return null;
              }
              catch (DataException x)
              { throw new WorkException("Error updating sequence",x);
              }
            }
            
          }.work();
          
        }
        else
        {
          final EditableTuple row=(EditableTuple) result.getTuple();
          next=(Long) resultRow.get("nextValue");
          increment=(Integer) resultRow.get("increment");
          
          stop=next+increment;
          row.set("nextValue",next+increment);

          final Tuple original=resultRow;
          new WorkUnit<Void>()
          {
            { this.nesting=Nesting.ISOLATE;
            }
            
            @Override
            protected Void run() throws WorkException
            {
              try
              {
                DataConsumer<DeltaTuple> updater=getUpdater(sequenceType,null);
                updater.dataInitialize(sequenceType.getFieldSet());
           
                updater.dataAvailable(new ArrayDeltaTuple(original,row));
                updater.dataFinalize();
                return null;
              }
              catch (DataException x)
              { throw new WorkException("Error updating sequence",x);
              }
            }
            
          }.work();
          
            
        }

                
      }
    }
    
    @Override
    public synchronized Long next()
      throws DataException
    {
      if (next==stop)
      { allocate();
      }
      return next++;
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

  @Override
  public URI getLocalResourceURI()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected Sequence createTxIdSequence()
  {
    return new SqlSequence
      (URI.create("class:/spiralcraft/sql/store/SqlStore.txId"));
  }

  
}
