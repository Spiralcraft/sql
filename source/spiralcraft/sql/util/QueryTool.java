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
package spiralcraft.sql.util;

import spiralcraft.exec.Executable;
import spiralcraft.exec.ExecutionContext;

import spiralcraft.sql.data.SerialResultSetCursor;

import spiralcraft.stream.Resolver;
import spiralcraft.stream.Resource;
import spiralcraft.stream.UnresolvableURIException;
import spiralcraft.stream.StreamUtil;

import spiralcraft.util.Arguments;

import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;

import spiralcraft.data.flatfile.Writer;

import spiralcraft.data.transport.DataConsumer;


import java.net.URI;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;

public class QueryTool
  implements Executable
{
  private ExecutionContext executionContext;
  private Connection connection;
  private DataSource dataSource;
  private URI dataSourceURI;
  private List<String> sqlList=new ArrayList<String>();
  private int fetchSize=1000;
  private boolean update=false;
  
  public void execute(final ExecutionContext context,String[] args)
  {
    executionContext=context;
    new Arguments()
    { 
      protected boolean processOption(String option)
      { 
        if (option=="database")
        { 
          String uri=nextArgument();
          try
          { setDataSource
              (new ResourceDataSource
                 (context.canonicalize
                   (URI.create(uri))
                 )
               );
          }
          catch (Exception x)
          { throw new IllegalArgumentException(uri+":"+x,x);
          }
        }
        else if (option=="script")
        { 
          String arg=nextArgument();
          try
          { 
            addScript(executionContext.canonicalize(URI.create(arg)));
          }
          catch (IOException x)
          { error("Error loading script '"+arg+"'",x);
          }
        }
        else if (option=="fetchSize")
        { 
          setFetchSize(Integer.parseInt(nextArgument()));
        }
        else if (option=="update")
        { setUpdate(true);
        }
        else
        { return false;
        }
        return true;
        
      }

      protected boolean processArgument(String argument)
      { 
        addSql(argument);
        return true;
      }
      
    }
    .process(args,'-');

    run();
  }
  
  
  public void setDataSource(DataSource ds)
  { dataSource=ds;
  }
  
  public void setUpdate(boolean val)
  { update=val;
  }
  
  public void addScript(URI scriptUri)
    throws IOException
  {
    Resource resource=Resolver.getInstance().resolve(scriptUri);
    InputStream in=resource.getInputStream();
    addSql(new String(StreamUtil.readBytes(in)));
    in.close();  
  }
  
  public void addSql(String sql)
  { sqlList.add(sql);
  }
    
  public void run()
  {
    if (dataSourceURI==null && dataSource==null)
    { 
      error("No database specified and no dataSource configured",null);
      return;
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
    {
      for (String sql : sqlList)
      { executeQuery(sql);
      }
    }
    catch (Exception x)
    { error("Caught exception executing sql",x);
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

  public void setFetchSize(int val)
  { fetchSize=val;
  }
   
 
  public void output(ResultSet rs)
    throws SQLException,IOException,DataException
  { 
    SerialResultSetCursor cursor=new SerialResultSetCursor(rs);
    
    DataConsumer<Tuple> writer=new Writer(executionContext.out());
    writer.dataInitialize(cursor.dataGetFieldSet());
    
    while (cursor.dataNext())
    { writer.dataAvailable(cursor.dataGetTuple());
    }
    writer.dataFinalize();
  }
  
  private void openConnection()
    throws IOException,SQLException,UnresolvableURIException
  { 
    connection=dataSource.getConnection();
    connection.setAutoCommit(false);

  }

  private void closeConnection()
    throws SQLException
  { connection.close();
  }
  
  private void executeQuery(String sql)
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
        try
        { output(rs);
        }
        catch (IOException x)
        { error("Error writing output for '"+sql+"'",x);
        }
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
  
  private void error(String message,Throwable cause)
  { 
    executionContext.err().println(message);
    if (cause!=null)
    { cause.printStackTrace(executionContext.err());
    }
  }
}