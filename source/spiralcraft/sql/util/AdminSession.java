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
package spiralcraft.sql.util;

import java.sql.SQLException;


/**
 * Provides general access to administrative database functions, when
 *   connected to an account with administrative privileges
 * 
 * @author mike
 *
 */
public abstract class AdminSession
  extends Session
{

  public abstract void createLogin(String loginName,String password)
    throws SQLException;

  public abstract void dropLogin(String loginName)
    throws SQLException;
  
  public abstract boolean loginExists(String loginName)
    throws SQLException;
  
  public abstract void dropDatabase(String databaseName)
    throws SQLException;
    
  public abstract boolean databaseExists(String databaseName)
    throws SQLException;
  
  public abstract void createDatabase(String databaseName,String ownerName)
    throws SQLException;


}
