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

import spiralcraft.data.lang.TupleFocus;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;

import spiralcraft.data.transport.SerialCursor;

import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.SqlStore;

import spiralcraft.lang.Focus;

public abstract class BoundSqlQuery<Tq extends Query>
  extends BoundQuery<Tq>
{
  protected final TupleFocus focus;
  protected final SqlStore store;
  protected boolean resolved;
  protected BoundQueryStatement statement;
  
  public BoundSqlQuery(Tq query,Focus parentFocus,SqlStore store)
    throws DataException
  { 
    focus=new TupleFocus(query.getFieldSet());
    if (parentFocus!=null)
    { focus.setParentFocus(parentFocus);
    }
    this.store=store;
    setQuery(query);
  }
  
  public abstract BoundQueryStatement composeStatement()
    throws DataException;

  public void resolve()
    throws DataException
  {
    if (resolved)
    { throw new IllegalStateException("Already resolved");
    }
    resolved=true;
    statement=composeStatement();
    statement.bindParameters(focus);
  }
  
  public SerialCursor execute()
    throws DataException
  { 
    if (!resolved)
    { resolve();
    }
    
    return statement.execute(); 
  }
}
