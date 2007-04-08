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

import spiralcraft.lang.Focus;

import spiralcraft.data.Field;
import spiralcraft.data.DataException;

import spiralcraft.data.query.Scan;


import spiralcraft.sql.data.store.SqlStore;
import spiralcraft.sql.data.store.TableMapping;
import spiralcraft.sql.data.store.BoundQueryStatement;

import spiralcraft.sql.dml.SelectStatement;
import spiralcraft.sql.dml.FromClause;
import spiralcraft.sql.dml.SelectList;
import spiralcraft.sql.dml.SelectListItem;

/**
 * A SQL implementatin of the basic Scan Query. 
 */
public class BoundScan
  extends BoundSqlQuery<Scan>
{
    
  public BoundScan(Scan query,Focus parentFocus,SqlStore store)
  { super(query,parentFocus,store);
  }
  
  public BoundQueryStatement composeStatement()
    throws DataException
  {
    BoundQueryStatement statement
      =new BoundQueryStatement(store,getQuery().getFieldSet());
    
    Scan scan=getQuery();
    
    SelectStatement select=new SelectStatement();
    
    if (scan.getType()==null)
    { throw new DataException("Scan Query must specify a Type when executed against a SqlStore");
    }
    
    TableMapping tableMapping=store.getTypeManager().getTableMapping(scan.getType());
    if (tableMapping==null)
    { 
      throw new DataException
        ("This store does not handle data for Type "+scan.getType().getURI());
    }
    statement.setPrimaryTableMapping(tableMapping);

    select.setFromClause
      (new FromClause
          (tableMapping.getSchemaName()
          ,tableMapping.getTableName()
          )
      );
    
    SelectList selectList=new SelectList();
    for (Field field : scan.getFieldSet().fieldIterable())
    { 
      SelectListItem[] items=tableMapping.createSelectListItems(field);
      
      // Possibly multiple DB columns for a single data Field.
      // Nested types use this, also complex primitives.
      for (SelectListItem item: items)
      { selectList.addItem(item);
      }
    }
    
    select.setSelectList(selectList);
    
    statement.setSqlFragment(select);
    return statement;
  }
  
}