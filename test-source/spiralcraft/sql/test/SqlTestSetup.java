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
package spiralcraft.sql.test;


import java.sql.SQLException;


import spiralcraft.cli.BeanArguments;
import spiralcraft.sql.util.AbstractSqlTool;
import spiralcraft.sql.util.AdminSession;

/**
 * Perform admin ops to setup a test database
 * 
 * @author mike
 *
 */
public class SqlTestSetup
  extends AbstractSqlTool
{
  private AdminSession adminSession;
  private String targetUsername;
  private String targetPassword;
  private String targetDatabase;
  
  
  @Override
  public void execute(String ... args)
  {
    new BeanArguments<SqlTestSetup>(this).process(args);
    run();
  }
    
  @Override
  protected void doWork()
    throws SQLException
  {
    adminSession.start(connection);
    log.fine("Checking for database "+targetDatabase);
    if (adminSession.databaseExists(targetDatabase))
    { 
      log.fine("Dropping database "+targetDatabase);
      adminSession.dropDatabase(targetDatabase);
    }
    log.fine("Checking for login "+targetUsername);
    if (adminSession.loginExists(targetUsername))
    { 
      log.fine("Dropping login "+targetUsername);
      adminSession.dropLogin(targetUsername);
    }
    
    log.fine("Creating login "+targetUsername);
    adminSession.createLogin(targetUsername,targetPassword);
    log.fine("Creating database "+targetDatabase);
    adminSession.createDatabase(targetDatabase,targetPassword);
  }
 

  public void setTargetUsername(String username)
  { this.targetUsername=username;
  }
  
  public void setTargetPassword(String password)
  { this.targetPassword=password;
  }

  public void setTargetDatabase(String database)
  { this.targetDatabase=database;
  }

  public void setAdminSession(AdminSession adminSession)
  { this.adminSession=adminSession;
  }
}
