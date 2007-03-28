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

import spiralcraft.data.transport.Space;
import spiralcraft.data.transport.Store;

import spiralcraft.data.DataException;
import spiralcraft.data.Type;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.TypeAccess;


import spiralcraft.sql.data.query.BoundSelection;
import spiralcraft.sql.data.query.BoundTypeAccess;

import spiralcraft.sql.ddl.DDLStatement;

import spiralcraft.registry.Registrant;
import spiralcraft.registry.RegistryNode;

import java.sql.PreparedStatement;
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
  
  public void setDataSource(DataSource dataSource)
  { this.dataSource=dataSource;
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
  { 
    TableMapping mapping=typeManager.getTableMapping(type);
    if (mapping==null)
    { throw new DataException("SqlStore: Unknown Type "+type.getURI());
    }
    return new BoundTypeAccess(mapping.getTypeAccess(),null,this);
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
        statement.write(buff,"");
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
      if (((Selection) query).getConstraints()!=null)
      { 
        // We can't deal with constraints just yet
        return query.solve(focus,getSpace());
      }
      else
      { return new BoundSelection((Selection) query,focus,this);
      }
    }
    else if (query instanceof TypeAccess)
    { return new BoundTypeAccess((TypeAccess) query,focus,this);
    }
    else
    { 
      // Solve it until we get something we can understand
      return query.solve(focus,getSpace());
    }
  }
  
  public TypeManager getTypeManager()
  { return typeManager;
  }
    
  
  /**
   * <P>Obtain a PreparedStatement that matches the specified sqlText,
   *  using either the Connection preallocated for the current Thread, or a
   *  newly allocated Connection owned by the returned PreparedStatement for
   *  as long as it is open.
   *  
   * <P>It is the responsibility of the caller to call
   *   PreparedStatement.close() in order to prevent a memory leak.
   *
   *@return a PreparedStatement instance
   */
  public PreparedStatement allocateStatement(String sqlText)
    throws DataException
  { 
    // TODO: Interim implementation
    // XXX: Connection allocation process must be designed
    try
    { return allocateConnection().prepareStatement(sqlText);
    }
    catch (SQLException x)
    { 
      throw new DataException
        ("Error preparing statement ["+sqlText+"]: "+x
        ,x
        );
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
  
  public Connection allocateConnection()
    throws DataException
  { 
    // TODO: Interim implementation
    // XXX: Connection allocation process must be designed
    try
    { return dataSource.getConnection();
    }
    catch (SQLException x)
    { throw new DataException("Error allocating connection: "+x,x);
    }
  }


  
  
}
