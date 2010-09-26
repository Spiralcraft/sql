//
package spiralcraft.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import spiralcraft.log.Level;
import spiralcraft.pool.Pool;
import spiralcraft.pool.ResourceFactory;
import spiralcraft.sql.jdbc.ConnectionWrapper;
import spiralcraft.sql.jdbc.StatementCachingConnection;
import spiralcraft.time.Scheduler;


/**
 * Provides a means for dispatching Runnables on their
 *   own Threads with support for Thread recycling to
 *   conserve resources.
 */
public class ConnectionPool
  extends Pool<ConnectionPool.PooledConnection>
  implements ResourceFactory<ConnectionPool.PooledConnection>
{

  private DataSource dataSource;
  
  
  { setResourceFactory(this);
  }

  public void setDataSource(DataSource dataSource)
  { this.dataSource=dataSource;
  }

  @Override
  public PooledConnection createResource()
    throws SQLException
  { 
    PooledConnection connection=new PooledConnection();
    return connection;
    
  }

  @Override
  public void discardResource(PooledConnection resource)
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
      return new StatementCachingConnection(dataSource.getConnection());
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
      if (!checkedIn)
      { 
        checkedIn=true;
      
        Scheduler.instance().scheduleNow
          (new Runnable()
          {
            @Override
            public void run()
            { 
              try
              {
                // Cleanup
                reset();
                ConnectionPool.this.checkin(PooledConnection.this);
              }
              catch (SQLException x)
              { 
                log.log
                  (Level.WARNING
                  ,"Could not reset or re-establish connection- "
                  +" discarding PooledConnection"
                  ,x
                  );
                ConnectionPool.this.discard(PooledConnection.this);
              }
            }
          
          }
          );
      }
    }
  }


}
