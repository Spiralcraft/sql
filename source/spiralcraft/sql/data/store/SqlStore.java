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

import spiralcraft.data.transport.DataConsumer;
import spiralcraft.data.transport.Space;
import spiralcraft.data.transport.Store;

import spiralcraft.data.DataException;
import spiralcraft.data.DeltaTuple;
import spiralcraft.data.Type;

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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import java.util.List;

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

  public void register(RegistryNode node)
  { 
    this.space=(Space) node.findInstance(Space.class);
    registryNode=node.createChild(SqlStore.class,this);
    RegistryNode childNode
      =registryNode.createChild("typeManager");
    typeManager.register(childNode);

  }
  
  public void initialize()
    throws DataException
  { 
    onAttach(); // XXX Waiting for auto-recovery implementation
  }

  public Space getSpace()
  { return space;
  }
  
  public boolean containsType(Type type)
  {
    // TODO Auto-generated method stub
    return typeManager.getTableMapping(type)!=null;
  }

  public BoundQuery getAll(Type type)
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
      
      conn=allocateConnection();
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
  
  public List<Type> getTypes()
  {
    // TODO Auto-generated method stub
    return null;
  }
  
  public BoundQuery query(Query query,Focus focus)
    throws DataException
  { 
    if (query instanceof Selection)
    { 
      BoundSelection boundSelection=new BoundSelection((Selection) query,focus,this);
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
  public DataConsumer<DeltaTuple> getUpdater(Type type)
    throws DataException
  { return assertTableMapping(type).getUpdater().newBatch();
  }
    
  
  /**
   * Allocate a Connection that is coordinated with the Transaction in-context, if 
   *   any.
   */
  public Connection allocateConnection()
    throws DataException
  { 
    Transaction transaction=Transaction.getContextTransaction();
    if (transaction!=null)
    { return resourceManager.branch(transaction).getConnection();
    }
    else
    {
      try
      { return dataSource.getConnection();
      }
      catch (SQLException x)
      { throw new DataException("Error allocating connection: "+x,x);
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

  private TableMapping assertTableMapping(Type type)
    throws DataException
  {    
    TableMapping mapping=typeManager.getTableMapping(type);
    if (mapping==null)
    { throw new DataException("SqlStore: Not a locally handled Type "+type.getURI());
    }
    return mapping;
  }
  
  
}
