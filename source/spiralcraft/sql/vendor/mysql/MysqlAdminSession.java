//
// Copyright (c) 2009,2009 Michael Toth
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
package spiralcraft.sql.vendor.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

import spiralcraft.log.Level;
import spiralcraft.sql.util.AdminSession;

public class MysqlAdminSession
    extends AdminSession
{

  @Override
  public void createLogin(String loginName, String password)
    throws SQLException
  { 
    boolean autoCommit=connection.getAutoCommit();
    connection.setAutoCommit(true);
    executeUpdate
      ("CREATE USER \""+loginName+"\" IDENTIFIED BY '"+password+"'"
      );
    connection.setAutoCommit(autoCommit);
    
  }
  
  @Override
  public void dropLogin(String loginName)
    throws SQLException
  { 
    
    boolean autoCommit=connection.getAutoCommit();
    connection.setAutoCommit(true);
    executePreparedUpdate
      ("DROP USER \""+loginName+"\"");
    connection.setAutoCommit(autoCommit);
    
  }


  @Override
  public void createDatabase(String databaseName, String ownerName)
      throws SQLException
  {    
    boolean autoCommit=connection.getAutoCommit();
    connection.setAutoCommit(true);
    executeUpdate
      ("CREATE DATABASE \""+databaseName+"\"");
    executeUpdate
      ("GRANT ALL ON \""+databaseName+"\".* TO \""+ownerName+"\"");
    connection.setAutoCommit(autoCommit);
    
  }

  @Override
  public void dropDatabase(String databaseName)
      throws SQLException
  { 
    boolean autoCommit=connection.getAutoCommit();
    connection.setAutoCommit(true);
    executeUpdate
      ("DROP DATABASE \""+databaseName+"\"");
    connection.setAutoCommit(autoCommit);
  }


  @Override
  public boolean databaseExists(String databaseName) throws SQLException
  {
    ResultSet rs=null;
    try
    {
      rs=executePreparedQuery
          ("SELECT count(*) AS count from db where db=?"
          ,databaseName
          );
    
      rs.next();
      return rs.getInt(1)>0;
    }
    finally
    { 
      if (rs!=null)
      { 
        try
        { rs.close();
        }
        catch (SQLException x)
        { log.log(Level.WARNING,"databaseExists()",x);
        }
      }
    }

  }  
  
  @Override
  public boolean loginExists(String loginName) 
    throws SQLException
  {
    ResultSet rs
      =executePreparedQuery
        ("SELECT count(*) AS count from user where user=?",loginName);
    
    rs.next();
    return rs.getInt(1)>0;
  }

}
