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

import spiralcraft.data.DataException;

import spiralcraft.data.query.Scan;


import spiralcraft.sql.data.store.SqlStore;
import spiralcraft.sql.data.store.TableMapping;
import spiralcraft.sql.data.store.BoundQueryStatement;

import spiralcraft.sql.dml.SelectStatement;


/**
 * A SQL implementatin of the basic Scan Query. 
 */
public class BoundScan
  extends BoundSqlQuery<Scan>
{
    
  private final TableMapping tableMapping;
  
  public BoundScan
    (Scan query
    ,Focus<?> parentFocus
    ,SqlStore store
    ,TableMapping table
    )
    throws DataException
  { 
    super(query,parentFocus,store);
    this.tableMapping=table;
  }
  
  public TableMapping getTableMapping()
  { return tableMapping;
  }
  
  @Override
  protected BoundQueryStatement composeStatement()
    throws DataException
  {
    BoundQueryStatement statement
      =new BoundQueryStatement(store,tableMapping.getType().getFieldSet());
    
    Scan scan=getQuery();
    
    
    if (scan.getType()==null)
    { 
      throw new DataException
        ("Scan Query must specify a Type when executed against a SqlStore");
    }
    
    statement.setPrimaryTableMapping(tableMapping);

    SelectStatement select=new SelectStatement();
    select.setFromClause(tableMapping.getFromClause());

    select.setSelectList(tableMapping.getSelectList());
    
    statement.setSqlFragment(select);
    statement.setResultSetMapping(tableMapping.getResultSetMapping());
    return statement;
  }
  
 
}