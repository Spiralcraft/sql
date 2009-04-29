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


import spiralcraft.lang.Focus;

import spiralcraft.data.DataConsumer;
import spiralcraft.data.access.Store;

import spiralcraft.data.DataException;
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.Sequence;
import spiralcraft.data.Type;
import spiralcraft.data.Tuple;
import spiralcraft.data.Space;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.Scan;

import spiralcraft.data.transaction.Transaction;

import spiralcraft.sql.data.query.BoundSelection;
import spiralcraft.sql.data.query.BoundScan;

import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.registry.Registrant;
import spiralcraft.registry.RegistryNode;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import java.util.List;

import spiralcraft.common.LifecycleException;


/**
 * A Store backed by a SQL database
 *
 */
public class SqlStore
  implements Store,Registrant
{
  
  private DataSource dataSource;
  private Space space;
  private TypeManager typeManager=new TypeManager();
  private RegistryNode registryNode;
  private SqlResourceManager resourceManager
    =new SqlResourceManager(this);
  
  /**
   * Specify the dataSource that will supply JDBC connections 
   */
  public void setDataSource(DataSource dataSource)
  { this.dataSource=dataSource;
  }
  

  public Sequence getSequence(URI sequenceURI)
  { 
    throw new UnsupportedOperationException("SQL Sequences not implemented");
    // XXX Need to implement this
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
  
  public void register(RegistryNode node)
  { 
    this.space=node.findInstance(Space.class);
    registryNode=node.createChild(SqlStore.class,this);
    RegistryNode childNode
      =registryNode.createChild("typeManager");
    typeManager.register(childNode);

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
  { return dataSource.getConnection();
  }
  
  @Override
  public void start()
    throws LifecycleException
  { 
    try
    { onAttach(); // XXX Waiting for auto-recovery implementation
    }
    catch (DataException x)
    { throw new LifecycleException(x.toString(),x);
    }
  }

  @Override
  public void stop()
  {
  }
  
  public Space getSpace()
  { return space;
  }
  
  public boolean containsType(Type<?> type)
  {
    // TODO Auto-generated method stub
    return typeManager.getTableMapping(type)!=null;
  }

  public BoundQuery<?,Tuple> getAll(Type<?> type)
    throws DataException
  { return new BoundScan(assertTableMapping(type).getScan(),null,this);
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
  
  public Type<?>[] getTypes()
  {
    // TODO Auto-generated method stub
    return null;
  }
  
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
    { 
      // Solve it until we get something we can understand
      return query.solve(focus,getSpace());
    }
  }
  
  
  /**
   * @return A DataConsumer which is used to push one or more updates into
   *   this Store. 
   */
  public DataConsumer<DeltaTuple> getUpdater(Type<?> type,Focus<?> focus)
    throws DataException
  { return assertTableMapping(type).getUpdater().newBatch(focus);
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
  
  
}
