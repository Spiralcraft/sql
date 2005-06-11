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

import java.net.URI;

import java.util.Properties;

import java.sql.Connection;
import java.sql.SQLException;

import java.io.IOException;

/**
 * A ConnectionManager which reads connection info from a resource
 *   and creates new Connections as they are  requested and closes them
 *   after the client is finished with them.
 */
public class ResourceConnectionManager
  implements ConnectionManager
{
  private DriverAgent agent;
  private URI databaseResourceURI;
  
  private Class driverClass;
  private Class vendorAdviceClass;
  private URI databaseURL;
  private String user;
  private String password;
  private Properties properties;

  public ResourceConnectionManager(URI resource)
    throws IOException,SQLException
  { setResourceURI(resource);
  }
  
  public ResourceConnectionManager()
  { }
  
  public void setResourceURI(URI resource)
    throws IOException,SQLException
  { 
    databaseResourceURI=resource;
    agent=
      new DriverAgent
        (new ResourceConnectionInfo(databaseResourceURI));
  }
  
  public Connection openConnection()
    throws SQLException
  { return agent.connect();
  }
  
  public void closeConnection(Connection conn)
    throws SQLException
  { conn.close();
  }
  
}