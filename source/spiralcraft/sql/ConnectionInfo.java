package spiralcraft.sql;

import java.net.URI;

import java.util.Properties;
/**
 * Information required by the DriverAgent to create a database connection
 */
public interface ConnectionInfo
{
  //XXX Implement change events
  
  public Class getDriverClass();
  
  public Class getVendorAdviceClass();
  
  public URI getDatabaseURL();
  
  public String getUser();
  
  public String getPassword();
  
  public Properties getProperties();
  
}