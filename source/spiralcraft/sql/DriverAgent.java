package spiralcraft.sql;

import java.util.Properties;

import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
 
/**
 * Interface between JDBC drivers and Spiralcraft tools which compensates
 *   for driver specific issues. Serves as an alternative method for
 *   instantiating and using JDBC drivers.
 */
public class DriverAgent
  implements Driver
{
  private static final String URL_PREFIX="spiralcraft:";
  
  private final ConnectionInfo connectionInfo;
  private final Driver driver;
  private final Properties properties;
  
  public DriverAgent(ConnectionInfo connectionInfo)
    throws SQLException
  { 
    this.connectionInfo=connectionInfo;
    properties=new Properties(connectionInfo.getProperties());

    if (connectionInfo.getUser()!=null)
    { properties.put("user",connectionInfo.getUser());
    }
    
    if (connectionInfo.getPassword()!=null)
    { properties.put("password",connectionInfo.getPassword());
    }
    
    if (connectionInfo.getDriverClass()==null)
    { 
      throw new SQLException
        ("Driver class not specified for "+connectionInfo.getDatabaseURL()
        );
    }
    try
    { driver=(Driver) connectionInfo.getDriverClass().newInstance();
    }
    catch (InstantiationException x)
    { throw new SQLException("Error loading driver: "+x.toString());
    }
    catch (IllegalAccessException x)
    { throw new SQLException("Error loading driver: "+x.toString());
    }
  } 
  
  public Connection connect(String url,Properties info)
    throws SQLException
  { return driver.connect(url,info);
  }

  public Connection connect()
    throws SQLException
  { 
    return driver.connect
      (connectionInfo.getDatabaseURL().toString()
      ,properties
      );
  }
  
  public boolean jdbcCompliant()
  { return driver.jdbcCompliant();
  }
  
  public int getMajorVersion()
  { return driver.getMajorVersion();
  }  
  
  public int getMinorVersion()
  { return driver.getMinorVersion();
  }
  
  public DriverPropertyInfo[] getPropertyInfo(String string,Properties props)
    throws SQLException
  { return driver.getPropertyInfo(string,props);
  }
  
  public boolean acceptsURL(String url)
    throws SQLException
  { return driver.acceptsURL("jdbc:"+url.substring(URL_PREFIX.length()+1));
  }
  
  
}