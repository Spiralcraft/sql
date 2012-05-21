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
package spiralcraft.sql.vendor.hsqldb;

import java.sql.ResultSet;
import java.sql.SQLException;

import spiralcraft.sql.util.AdminSession;

public class HsqldbAdminSession
    extends AdminSession
{


  @Override
  public void createLogin(String loginName, String password)
    throws SQLException
  { 
    executeUpdate
      ("CREATE ROLE \""+loginName+"\" LOGIN PASSWORD '"+password+"'"
      );
    
  }
  
  @Override
  public void dropLogin(String loginName)
    throws SQLException
  { 
    
    executePreparedUpdate
      ("DROP ROLE \""+loginName+"\"");
    
  }


  @Override
  public void createDatabase(String databaseName, String ownerName)
      throws SQLException
  {    
    boolean autoCommit=connection.getAutoCommit();
    connection.setAutoCommit(true);
    executeUpdate
      ("CREATE DATABASE \""+databaseName+"\" OWNER \""+ownerName+"\"");
    connection.setAutoCommit(autoCommit);
    
  }

  @Override
  public void dropDatabase(String databaseName)
      throws SQLException
  { return;   
  }

  @Override
  public boolean databaseExists(String databaseName) throws SQLException
  { return true;
  }

  @Override
  public boolean loginExists(String loginName) 
    throws SQLException
  {
    ResultSet rs
      =executePreparedQuery
        ("SELECT count(*) AS count from pg_roles where rolname=?",loginName);
    
    rs.next();
    return rs.getInt(1)>0;
  }

}
