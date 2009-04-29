//
// Copyright (c) 1998,2009 Michael Toth
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
package spiralcraft.sql.util;

import spiralcraft.exec.Executable;
import spiralcraft.exec.ExecutionContext;

import spiralcraft.sql.data.SerialResultSetCursor;


import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;

import spiralcraft.data.flatfile.Writer;
import spiralcraft.data.persist.PersistenceException;

import spiralcraft.data.DataConsumer;


import java.net.URI;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import javax.sql.DataSource;


/**
 * Base class for Executable tools that interact with databases
 * 
 * @author mike
 *
 */
public abstract class AbstractSqlTool
  implements Executable
{
  protected Connection connection;
  private DataSource dataSource;
  private URI dataSourceURI;
  private int fetchSize=1000;
 
  
  public void setDataSourceURI(URI dataSourceURI)
  { this.dataSourceURI=dataSourceURI;
  }
  
  public void setDataSource(DataSource ds)
  { dataSource=ds;
  }
  
    
  public final void run()
  {
    
    if (dataSourceURI==null && dataSource==null)
    { 
      error("No database specified and no dataSource configured",null);
      return;
    }

    if (dataSourceURI!=null)
    {
      try
      {
        setDataSource
          (new ResourceDataSource
            (ExecutionContext.getInstance().canonicalize(dataSourceURI)
            )
          );
      }
      catch (PersistenceException x)
      { 
        throw new RuntimeException
          ("Could not load DataSource object from "+dataSourceURI,x);
      }
    }
    
    if (dataSource==null)
    { 
      error("No DataSource configured",null);
      return;
    }
    
    try
    { openConnection();
    }
    catch (Exception x)
    { 
      error("Error opening database connection",x);
      return;
    }
    
    try
    { doWork();
    }
    catch (SQLException x)
    { error("Caught exception doing work",x);
    }
    
    try
    { closeConnection();
    }
    catch (SQLException x)
    { 
      error("Error closing database connection",x);
      return;
    }
    
  }

  protected abstract void doWork()
    throws SQLException;
  
  public void setFetchSize(int val)
  { fetchSize=val;
  }
   
 
  public void output(ResultSet rs)
    throws DataException
  { 
    SerialResultSetCursor cursor=new SerialResultSetCursor(rs);
    
    DataConsumer<Tuple> writer=new Writer(ExecutionContext.getInstance().out());
    writer.dataInitialize(cursor.getFieldSet());
    
    while (cursor.next())
    { writer.dataAvailable(cursor.getTuple());
    }
    writer.dataFinalize();
  }
  
  private void openConnection()
    throws SQLException
  { 
    connection=dataSource.getConnection();
    connection.setAutoCommit(false);

  }

  private void closeConnection()
    throws SQLException
  { connection.close();
  }
  
  protected void executeQuery(String sql,boolean update)
    throws DataException
  { 
    Statement st=null;
    ResultSet rs=null;
    try
    {
      st=connection.createStatement(); 
      st.setFetchSize(fetchSize);
      if (update)
      { st.executeUpdate(sql);
      }
      else
      { 
        rs=st.executeQuery(sql);
        rs.setFetchSize(fetchSize);
        output(rs);
      }
      connection.commit();
    }
    catch (SQLException x)
    { error("Error executing query '"+sql+"'",x);
    }
    finally
    { 
      try
      {
        if (rs!=null)
        { rs.close();
        }
        if (st!=null)
        { st.close();
        }
      }
      catch (SQLException x)
      { error("Error cleaning up after '"+sql+"'",x);
      }
     
    }
    
  }
  
  protected void error(String message,Throwable cause)
  { 
    ExecutionContext.getInstance().err().println(message);
    if (cause!=null)
    { cause.printStackTrace(ExecutionContext.getInstance().err());
    }
  }
}