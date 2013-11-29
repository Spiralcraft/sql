//
// Copyright (c) 1998,2007 Michael Toth
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
package spiralcraft.sql.data.query;

import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;

import spiralcraft.data.lang.CursorBinding;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;
import spiralcraft.data.spi.ManualCursor;

import spiralcraft.data.access.SerialCursor;

import spiralcraft.sql.data.SerialResultSetCursor;
import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.SqlStore;

import spiralcraft.lang.BindException;
import spiralcraft.lang.Focus;
import spiralcraft.lang.SimpleFocus;

public abstract class BoundSqlQuery<Tq extends Query>
  extends BoundQuery<Tq,Tuple>
{
  protected final Focus<?> focus;
  protected final SqlStore store;
  protected boolean resolved;
  protected BoundQueryStatement statement;
  
  @SuppressWarnings({ "unchecked", "rawtypes"
    })
  public BoundSqlQuery(Tq query,Focus<?> parentFocus,SqlStore store)
    throws DataException
  { 
    super(query,parentFocus);
    try
    {
      if (parentFocus!=null)
      {
        focus=parentFocus.telescope
          (new CursorBinding
            (new ManualCursor(query.getFieldSet()))
          );
      }
      else
      { 
        focus=
          new SimpleFocus
            (new CursorBinding(new ManualCursor(query.getFieldSet())));
      }
    }
    catch (BindException x)
    { throw new DataException("Error",x);
    }
        
    this.store=store;
  }
  
  protected abstract BoundQueryStatement composeStatement()
    throws DataException;


  
  @Override
  public void resolve()
    throws DataException
  {
    if (resolved)
    { return;
    }
    resolved=true;
    statement=composeStatement();
    statement.bindParameters(focus);
  }
  
  @Override
  protected SerialCursor<Tuple> doExecute()
    throws DataException
  { 
    
    Object[] parameterKey=statement.makeParameterKey();

    SerialResultSetCursor cursor
        =statement.execute(parameterKey); 
    return cursor;
  }
}
