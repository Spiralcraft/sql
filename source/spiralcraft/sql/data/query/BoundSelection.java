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

import java.util.List;

import spiralcraft.lang.Focus;

import spiralcraft.data.DataException;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.Query;



import spiralcraft.sql.data.store.SqlStore;
import spiralcraft.sql.dml.SelectStatement;

/**
 * A SQL optimized implementatin of the Selection Query. 
 */
public class BoundSelection
  extends BoundSqlQuery<Selection>
{
  // XXX: We still don't read the where clause
  
  private final BoundQuery source;

  
  public BoundSelection(Selection selection,Focus parentFocus,SqlStore store)
    throws DataException
  { super(selection,parentFocus,store);
    
    List<Query> sources=selection.getSources();
    if (sources.size()<1)
    { throw new DataException(getClass().getName()+": No source to bind to");
    }
    
    if (sources.size()>1)
    { throw new DataException(getClass().getName()+": Can't bind to more than one source");
    }

    this.source=store.query(sources.get(0),parentFocus);
  }
    
  public SelectStatement composeStatement()
    throws DataException
  {
    if (source instanceof BoundTypeAccess)
    {
      SelectStatement select=((BoundTypeAccess) source).composeStatement();
      
      // Read and interpret where clause here
      //
      //
      
      return select;
    }
    else
    { throw new DataException("Cannot SQL Select from anything but a TypeAccess source");
    }
      
    
  }
  

}