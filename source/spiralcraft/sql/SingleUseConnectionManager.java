package spiralcraft.sql;

import java.net.URI;

import java.util.Properties;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A ConnectionManager which creates new Connections as they are
 *   requested and closes them after the client is finished with them.
 */
public class SingleUseConnectionManager
  implements ConnectionManager,ConnectionInfo
{
  private DriverAgent agent;
  private Class driverClass;
  private Class vendorAdviceClass;
  private URI databaseURL;
  private String user;
  private String password;
  private Properties properties;

  public Connection openConnection()
    throws SQLException
  { 
    if (agent==null)
    { agent=new DriverAgent(this);
    }
    return agent.connect();
  }
  
  public void closeConnection(Connection conn)
    throws SQLException
  { conn.close();
  }
  
  public Class getDriverClass()
  { return driverClass;
  }
  
  public void setDriverClass(Class val)
  { this.driverClass=val;
  }
  
  public Class getVendorAdviceClass()
  { return vendorAdviceClass;
  }
  
  public void setVendorAdviceClass(Class val)
  { this.vendorAdviceClass=val;
  }
  
  public URI getDatabaseURL()
  { return databaseURL;
  }

  public void setDatabaseURL(URI val)
  { this.databaseURL=val;
  }
    
  public String getUser()
  { return user;
  }
  
  public void setUser(String user)
  { this.user=user;
  }
  
  public String getPassword()
  { return password;
  }

  public void setPassword(String password)
  { this.password=password;
  }
  
  public Properties getProperties()
  { return properties;
  }
  
  public void setProperties(Properties val)
  { this.properties=val;
  }
    
}