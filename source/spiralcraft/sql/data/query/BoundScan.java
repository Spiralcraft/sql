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
import spiralcraft.sql.data.store.ColumnMapping;
import spiralcraft.sql.data.store.BoundQueryStatement;

import spiralcraft.sql.dml.SelectStatement;
import spiralcraft.sql.dml.FromClause;
import spiralcraft.sql.dml.SelectList;
import spiralcraft.sql.dml.SelectListItem;

import spiralcraft.util.tree.LinkedTree;

/**
 * A SQL implementatin of the basic Scan Query. 
 */
public class BoundScan
  extends BoundSqlQuery<Scan>
{
    
  public BoundScan(Scan query,Focus<?> parentFocus,SqlStore store)
    throws DataException
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
    LinkedTree<Integer> foldTree=new LinkedTree<Integer>();
    LinkedTree<ColumnMapping> columnTree=tableMapping.getColumnMappingTree();
    int columnCount=0;
    generateSelectList(columnTree,selectList,foldTree,columnCount);
    
    select.setSelectList(selectList);
    
    statement.setSqlFragment(select);
    statement.setFoldTree(foldTree);
    return statement;
  }
  
  private int generateSelectList
    (LinkedTree<ColumnMapping> columnTree
    ,SelectList selectList
    ,LinkedTree<Integer> foldTree
    ,int columnCount
    )
  {
    for (LinkedTree<ColumnMapping> mapping : columnTree)
    { columnCount=generateSelectListItem(mapping,selectList,foldTree,columnCount);
    }
    return columnCount;    
  }
  
  private int generateSelectListItem
    (LinkedTree<ColumnMapping> node
    ,SelectList selectList
    ,LinkedTree<Integer> foldTree
    ,int columnCount
    )
  {
    if (node.isLeaf())
    {
      SelectListItem selectListItem=node.get().getSelectListItem();
      // Single field
      if (selectListItem!=null)
      { 
        selectList.addItem(selectListItem);
        foldTree.addChild(new LinkedTree<Integer>(columnCount++));
      }
      else
      { foldTree.addChild(new LinkedTree<Integer>());
      }
    }
    else
    { 
      LinkedTree<Integer> child=new LinkedTree<Integer>();
      foldTree.addChild(child);
      columnCount=generateSelectList(node,selectList,child,columnCount);
    }
    
    return columnCount;
  }
  
}