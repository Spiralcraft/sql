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