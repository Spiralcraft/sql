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
package spiralcraft.sql.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.SQLWarning;
import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.sql.Savepoint;
import java.sql.Struct;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLXML;

import java.util.Map;
import java.util.Properties;

/**
 * Wraps a connection by delegating all methods
 */
public abstract class ConnectionWrapper
  implements Connection
{
  protected Connection connection;

  /**
   *@see Connection.nativeSQL()
   */
  @Override
  public String nativeSQL(String sql)
    throws SQLException
  { return connection.nativeSQL(sql);
  } 

  @Override
  public void close()
    throws SQLException
  { connection.close();
  }

  @Override
  public void setTransactionIsolation(int iso)
    throws SQLException
  { connection.setTransactionIsolation(iso);
  }

  @Override
  public int getTransactionIsolation()
    throws SQLException
  { return connection.getTransactionIsolation();
  }

  @Override
  public Statement createStatement()
    throws SQLException
  { return connection.createStatement();
  }

  @Override
  public Statement createStatement(int a,int b)
    throws SQLException
  { return connection.createStatement(a,b);
  }

  @Override
  public PreparedStatement prepareStatement(String stmt)
    throws SQLException
  { return connection.prepareStatement(stmt);

  }

  @Override
  public PreparedStatement prepareStatement(String stmt,int a,int b)
    throws SQLException
  { return connection.prepareStatement(stmt,a,b);
  }

  @Override
  public boolean isReadOnly()
    throws SQLException
  { return connection.isReadOnly();
  }

  @Override
  public Struct createStruct(String typeName,Object[] attribs)
    throws SQLException
  { return connection.createStruct(typeName,attribs);
  }
  
  @Override
  public Array 	createArrayOf(String typeName, Object[] elements)
    throws SQLException
  { return connection.createArrayOf(typeName,elements);
  }
  
  @Override
  public Blob createBlob()
    throws SQLException
  { return connection.createBlob();
  }
  
  @Override
  public Clob createClob()
    throws SQLException
  { return connection.createClob();
  }

  @Override
  public NClob createNClob()
    throws SQLException
  { return connection.createNClob();
  }

  @Override
  public SQLXML createSQLXML()
    throws SQLException
  { return connection.createSQLXML();
  }
  
  @Override
  public Properties getClientInfo()
    throws SQLException
  { return connection.getClientInfo();
  }
  
  @Override
  public String getClientInfo(String name)
    throws SQLException
  { return connection.getClientInfo(name);
  }

  @Override
  public void setClientInfo(String name,String value)
    throws SQLClientInfoException
  { connection.setClientInfo(name,value);
  }
  
  @Override
  public void setClientInfo(Properties props)
    throws SQLClientInfoException
  { connection.setClientInfo(props);
  }
  
  @Override
  public CallableStatement prepareCall(String call)
    throws SQLException
  { return connection.prepareCall(call);
  }

  @Override
  public CallableStatement prepareCall(String call,int a,int b)
    throws SQLException
  { return connection.prepareCall(call,a,b);
  }

  @Override
  public DatabaseMetaData getMetaData()
    throws SQLException
  { return connection.getMetaData();
  }

  @Override
  public boolean isValid(int timeout)
    throws SQLException
  { return connection.isValid(timeout);
  }
  
  @Override
  public void rollback()
    throws SQLException
  { connection.rollback();
  }

  @Override
  public void setAutoCommit(boolean ac)
    throws SQLException
  { connection.setAutoCommit(ac);
  }

  @Override
  public boolean getAutoCommit()
    throws SQLException
  { return connection.getAutoCommit();
  }

  @Override
  public void clearWarnings()
    throws SQLException
  { connection.clearWarnings();
  }

  @Override
  public void setCatalog(String cat)
    throws SQLException
  { connection.setCatalog(cat);
  }

  @Override
  public String getCatalog()
    throws SQLException
  { return connection.getCatalog();
  }

  @Override
  public void commit()
    throws SQLException
  { connection.commit();
  }

  @Override
  public boolean isClosed()
    throws SQLException
  { return connection.isClosed(); 
  }

  @Override
  public void setReadOnly(boolean b)
    throws SQLException
  { connection.setReadOnly(b);
  }

  @Override
  public SQLWarning getWarnings()
    throws SQLException
  { return connection.getWarnings();
  }

  @Override
  public void setTypeMap(Map<String,Class<?>> map)
    throws SQLException
  { connection.setTypeMap(map);
  }

  @Override
  public Map<String,Class<?>> getTypeMap()
    throws SQLException
  { return connection.getTypeMap();
  }

  @Override
  public void setHoldability(int holdability) throws SQLException
  { connection.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException
  { return connection.getHoldability();
  }

  @Override
  public boolean isWrapperFor(Class<?> clazz)
    throws SQLException
  { return connection.isWrapperFor(clazz);
  }
  
  @Override
  public <T> T unwrap(Class<T> iface)
    throws SQLException
  { return connection.unwrap(iface);
  }
  
  @Override
  public Savepoint setSavepoint() throws SQLException
  { return connection.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException
  { return connection.setSavepoint(name);
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  { connection.rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  { connection.releaseSavepoint(savepoint);
  }

  
  @Override
  public Statement createStatement
    (int resultSetType
    ,int resultSetConcurrency
    ,int resultSetHoldability
    ) throws SQLException
  { return connection.createStatement(resultSetType,resultSetConcurrency,resultSetHoldability);
  }
  


  @Override
  public PreparedStatement prepareStatement
    (String sql
    ,int resultSetType
    ,int resultSetConcurrency
    ,int resultSetHoldability
    ) 
    throws SQLException
  { return connection.prepareStatement(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
  }

  @Override
  public CallableStatement prepareCall
    (String sql
    ,int resultSetType
    ,int resultSetConcurrency
    ,int resultSetHoldability
    ) 
    throws SQLException
  { return connection.prepareCall(sql,resultSetType,resultSetConcurrency,resultSetHoldability);
  }


  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
  	throws SQLException
  { return connection.prepareStatement(sql,autoGeneratedKeys);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int columnIndexes[])
  	throws SQLException
  { return connection.prepareStatement(sql,columnIndexes);
  }

  @Override
  public  PreparedStatement prepareStatement(String sql, String columnNames[])
  	throws SQLException
  { return connection.prepareStatement(sql,columnNames);
  }

}
