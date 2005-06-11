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
package spiralcraft.sql;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for tools which use database connections to obtain and
 *   release Connections.
 */
public interface ConnectionManager
{
  /**
   * Obtain a database connection
   */
  public Connection openConnection()
    throws SQLException;
  
  /**
   * Close a database connection
   */
  public void closeConnection(Connection connection)
    throws SQLException;
  
}