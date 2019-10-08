//
package spiralcraft.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import spiralcraft.common.callable.Sink;
import spiralcraft.log.Level;
import spiralcraft.pool.Pool;
import spiralcraft.pool.ResourceFactory;
import spiralcraft.sql.Dialect;
import spiralcraft.sql.jdbc.ConnectionWrapper;
import spiralcraft.sql.jdbc.StatementCachingConnection;


/**
 * A Pool which manages sql database connectionss
 *
 * @author mike
 *
 * @param <T>
 */
public class ConnectionPool<T extends Connection>
  extends Pool<ConnectionPool<T>.PooledConnection>
  implements ResourceFactory<ConnectionPool<T>.PooledConnection>
{

  private CommonDataSource dataSource;
  private Dialect dialect;
  private boolean xa;
  private ConnectionFactory<T> connectionFactory
    =new ConnectionFactory<T>()
    {
      @SuppressWarnings("unchecked")
      @Override
      public T newConnection(Connection delegate)
        throws SQLException
      { return (T) delegate;
      }
      
      @SuppressWarnings("unchecked")
      @Override
      public T newConnection(Connection delegate,XAConnection ca)
        throws SQLException
      { return (T) delegate;
      }
    };
  
  
  { 
    fastCheckin=true;
    setResourceFactory(this);
    setOnCheckout
      (new Sink<PooledConnection>()
        { 
          @Override
          public void accept(PooledConnection connection) 
          { connection.checkedIn=false;
          }
        }
      );
    setOnCheckin
      (new Sink<PooledConnection>()
        { 
          @Override
          public void accept(PooledConnection connection) 
          { 
            try
            { connection.reset();
            }
            catch (SQLException e)
            {
              // TODO Auto-generated catch block
              log.log(Level.WARNING,"Error resetting connection "+connection,e);
              ConnectionPool.this.discard(connection);
            }
          }
        }
      );
    
  }

  public void setDataSource(CommonDataSource dataSource)
  { 
    this.dataSource=dataSource;
    this.xa=dataSource instanceof XADataSource;
  }
  
  public void setDialect(Dialect dialect)
  { this.dialect=dialect;
  }

  @Override
  public ConnectionPool<T>.PooledConnection createResource()
    throws SQLException
  { 
    PooledConnection connection=new PooledConnection();
    return connection;
  }

  @Override
  public void discardResource(ConnectionPool<T>.PooledConnection resource)
  { 
    try
    { resource.getConnection().close();
    }
    catch (SQLException x)
    { log.log(Level.WARNING,"Error closing connection",x);
    }
  }

  class PooledConnection
    extends ConnectionWrapper
  {
    private boolean checkedIn=true;
    private boolean defaultAutoCommit;

    public PooledConnection()
      throws SQLException
    { 
      this.connection=newConnection();
      this.defaultAutoCommit=connection.getAutoCommit();
    }
    
    private Connection newConnection()
      throws SQLException
    {
      if (!xa)
      {
        Connection nativeConn=((DataSource) dataSource).getConnection();
        if (logLevel.isFine())
        { log.fine("Connected: "+nativeConn);
        }
        if (dialect!=null)
        { dialect.initConnection(nativeConn);
        }
        Connection ret=new StatementCachingConnection(nativeConn);
        return connectionFactory.newConnection(ret);
      }
      else
      { 
        XAConnection xa
          =((XADataSource) dataSource).getXAConnection();
        Connection nativeConn=xa.getConnection();
        if (logLevel.isFine())
        { log.fine("Connected: "+xa+" : "+nativeConn);
        }
        if (dialect!=null)
        { dialect.initConnection(nativeConn);
        }
        Connection ret=new StatementCachingConnection(nativeConn);
        return connectionFactory.newConnection(ret,xa);
      }
    }
    
    public Connection getConnection()
    { return connection;
    }
    
    private void reset()
      throws SQLException
    {
      try
      {
        connection.clearWarnings();
        
        if (connection.getAutoCommit()!=this.defaultAutoCommit)
        { connection.setAutoCommit(defaultAutoCommit);
        }
        else if (!this.defaultAutoCommit)
        { connection.rollback();
        }
      }
      catch (SQLException x)
      {
        log.log(Level.WARNING,"Discarding connection- error resetting",x);
        try
        { connection.close();
        }
        catch (SQLException y)
        { }
        
        this.connection=newConnection();
      }
    }
    
    @Override
    public void close()
    { 
      if (logLevel.isFine())
      { log.fine("Connection Closed: "+toString());
      }
      if (!checkedIn)
      { 
        checkedIn=true;
        ConnectionPool.this.checkin(PooledConnection.this);
     
      }
    }
  }

  public void setConnectionFactory
    (ConnectionFactory<T> connectionFactory)
  { this.connectionFactory=connectionFactory;
  }


}
