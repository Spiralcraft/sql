package spiralcraft.sql;

import java.net.URI;

import java.util.Properties;

import spiralcraft.stream.Resource;
import spiralcraft.stream.Resolver;
import spiralcraft.stream.UnresolvableURIException;

import java.sql.SQLException;

import java.io.InputStream;
import java.io.IOException;


/**
 * Reads connection info in java.util.Properties format from a resource
 */
public class ResourceConnectionInfo
  implements ConnectionInfo
{
  
  private Properties props;
  private Class driverClass;
  private Class vendorAdviceClass;
  private URI databaseURL;
  private String user;
  private String password;

  public ResourceConnectionInfo(URI resourceURI)
    throws IOException,SQLException,UnresolvableURIException
  { this(Resolver.getInstance().resolve(resourceURI));
  }
  
  public ResourceConnectionInfo(Resource resource)
    throws IOException,SQLException
  { 
    props=new Properties();
    InputStream in=resource.getInputStream();
    props.load(in);
    in.close();
    
    try
    {
      String driverClassName=props.getProperty("driverClass");
      if (driverClassName!=null)
      { 
        driverClass
          =Thread.currentThread().getContextClassLoader()
            .loadClass(driverClassName);
      }
    }
    catch (ClassNotFoundException x)
    { throw new SQLException("Driver class not found: "+x.toString());
    }
      
    try
    {
      String vendorAdviceClassName=props.getProperty("vendorAdviceClass");
      if (vendorAdviceClassName!=null)
      { 
        vendorAdviceClass
          =Thread.currentThread().getContextClassLoader()
            .loadClass(vendorAdviceClassName);
      }
    }
    catch (ClassNotFoundException x)
    { throw new SQLException("VendorAdvice class not found: "+x.toString());
    }
    
    String url=props.getProperty("url");
    if (url!=null)
    { databaseURL=URI.create(url);
    }
    
    String user=props.getProperty("user");
    String password=props.getProperty("password");
  }

  
  public Class getDriverClass()
  { return driverClass;
  }
  
  public Class getVendorAdviceClass()
  { return vendorAdviceClass;
  }
  
  public URI getDatabaseURL()
  { return databaseURL;
  }
  
  public String getUser()
  { return user;
  }
  
  public String getPassword()
  { return password;
  }
  
  public Properties getProperties()
  { return props;
  }

}