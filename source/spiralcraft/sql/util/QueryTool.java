//
// Copyright (c) 1998,2005 Michael Toth
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

import spiralcraft.sql.ResourceConnectionManager;
import spiralcraft.sql.ConnectionManager;
import spiralcraft.sql.types.TypeMap;

import spiralcraft.stream.Resolver;
import spiralcraft.stream.Resource;
import spiralcraft.stream.UnresolvableURIException;
import spiralcraft.stream.StreamUtil;

import spiralcraft.util.Arguments;

import spiralcraft.data.flatfile.Writer;
import spiralcraft.data.TypeResolver;
import spiralcraft.data.DataException;

import spiralcraft.data.spi.EditableArrayTuple;

import spiralcraft.data.pipeline.DataConsumer;

import spiralcraft.data.core.SchemeImpl;
import spiralcraft.data.core.FieldImpl;

import java.net.URI;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.InputStream;

public class QueryTool
  implements Executable
{
  private ExecutionContext executionContext;
  private Connection connection;
  private ConnectionManager connectionManager;
  private List<String> sqlList=new ArrayList<String>();
  private int fetchSize=1000;
  private boolean update=false;
  
  public void execute(ExecutionContext context,String[] args)
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
          {
            connectionManager
              =new ResourceConnectionManager(URI.create(uri));
          }
          catch (Exception x)
          { throw new IllegalArgumentException("database="+uri,x);
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
  
  public void setConnectionManager(ConnectionManager manager)
  { connectionManager=manager;
  }
  
  public void run()
  {
    if (connectionManager==null)
    { 
      error("No ConnectionManager configured",null);
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
   
 
  public void outputToTab(ResultSet rs)
    throws SQLException,IOException,DataException
  { 
    ResultSetMetaData md=rs.getMetaData();
    int count=md.getColumnCount();
    SchemeImpl fields=new SchemeImpl();
    for (int i=1;i<count+1;i++)
    { 
      FieldImpl field=new FieldImpl();
      field.setName(md.getColumnName(i));
      
      Class typeClass=TypeMap.getJavaClassFromSqlType(md.getColumnType(i));
      if (typeClass==null)
      { typeClass=Object.class;
      }
      
      field.setType
        (TypeResolver.getTypeResolver()
              .resolveFromClass(typeClass)
        );
      fields.addField(field);
    }
    
    DataConsumer writer=new Writer(executionContext.out());
    writer.dataInitialize(fields);
    while (rs.next())
    { 
      EditableArrayTuple data=new EditableArrayTuple(fields);
      for (int i=1;i<count+1;i++)
      { 
        try
        { data.set(i-1,rs.getObject(i));
        }
        catch (SQLException x)
        {
          // Deal with known corner cases
          int type=md.getColumnType(i);
          switch (type)
          {
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
              data.set(i-1,new String(rs.getBytes(i)));
              break;
            default:
              throw x;
          }
        }
      }
      writer.dataAvailable(data);
    }
    writer.dataFinalize();
  }
  
  private void openConnection()
    throws IOException,SQLException,UnresolvableURIException
  { 
    connection=connectionManager.openConnection();
    connection.setAutoCommit(false);

  }

  private void closeConnection()
    throws SQLException
  { 
    connectionManager.closeConnection(connection);
    connection=null;
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
        { outputToTab(rs);
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