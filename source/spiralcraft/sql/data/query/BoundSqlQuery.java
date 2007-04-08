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

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Query;

import spiralcraft.data.transport.SerialCursor;

import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.SqlStore;

import spiralcraft.lang.Focus;
import spiralcraft.lang.DefaultFocus;

public abstract class BoundSqlQuery<Tq extends Query>
  extends BoundQuery<Tq>
{
  protected final Focus parentFocus;
  protected final SqlStore store;
  
  protected boolean resolved;
  protected BoundQueryStatement statement;
  
  public BoundSqlQuery(Tq query,Focus parentFocus,SqlStore store)
  { 
    if (parentFocus==null)
    { 
      // XXX The default Focus should be something standard to Data 
      this.parentFocus=new DefaultFocus();
    }
    else
    { this.parentFocus=parentFocus;
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
    statement.bindParameters(parentFocus);
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
