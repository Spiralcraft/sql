package spiralcraft.sql.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import spiralcraft.data.transaction.Transaction;
import spiralcraft.data.transaction.TransactionException;
import spiralcraft.log.ClassLog;

/**
 * A ConnectionWrapper which begins an XA transaction on instantiation and
 *   completes it when "close" is called.
 * 
 * @author mike
 *
 */
public class XATransactionalConnection
  extends ConnectionWrapper
  implements Connection
{
  private static final ClassLog log
    =ClassLog.getInstance(XATransactionalConnection.class);
  
  private final XAConnection xa;
  private Xid xid;
  private Transaction localTx;
  boolean committed=true;
  
  public XATransactionalConnection(XAConnection xaConnection)
    throws SQLException
  {
    connection=xaConnection.getConnection();
    xa=xaConnection;
    localTx=Transaction.startContextTransaction(Transaction.Nesting.PROPOGATE);
    xid=localTx.nextXid();
    try
    { xa.getXAResource().start(xid,XAResource.TMNOFLAGS);
    }
    catch (XAException e)
    { throw new SQLException(e);
    }
  }
  
  @Override
  public void setAutoCommit(boolean autoCommit)
  { log.fine("Ignoring setAutoCommit("+autoCommit+") during XA transaction");
  }
  
  @Override
  public void commit() 
    throws SQLException
  {
    

    super.commit();
    try
    { 
      localTx.commit();
      xa.getXAResource().end(xid,XAResource.TMSUCCESS);
      xa.getXAResource().commit(xid,true);
      committed=true;
    }
    catch (XAException x)
    { throw new SQLException(x);
    }
    catch (TransactionException x)
    { throw new SQLException(x);
    }
  }
  
  @Override
  public void close()
    throws SQLException
  {
    if (!committed)
    { commit();
    }
    localTx.complete();
    super.close();
  }
}
