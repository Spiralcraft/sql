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
package spiralcraft.sql.data;


import spiralcraft.data.FieldSet;
import spiralcraft.data.Field;
import spiralcraft.data.Scheme;
import spiralcraft.log.ClassLog;

import spiralcraft.util.tree.LinkedTree;

import java.util.Iterator;

/**
 * Maps a result set to a Tuple structure
 */
public class ResultSetMapping
{
  
  private static final ClassLog log
    =ClassLog.getInstance(ResultSetMapping.class);

  final ResultSetMapping[] subs;
  final ResultSetMapping baseExtent;
  final ResultColumnMapping[] map;
  final FieldSet fieldSet;
  int totalColumnCount;
  
  private ResultSetMapping(FieldSet fieldSet)
  {
    this.fieldSet
      =fieldSet instanceof Scheme
      ?fieldSet
      :fieldSet.getType().getScheme()
      ;
    map=new ResultColumnMapping[fieldSet.getFieldCount()];
    subs=new ResultSetMapping[fieldSet.getFieldCount()];
    if (fieldSet.getType()!=null)
    {
      if (fieldSet.getType().getBaseType()!=null)
      { 
        FieldSet baseScheme=fieldSet.getType().getBaseType().getScheme();
        if (baseScheme!=null)
        { baseExtent=new ResultSetMapping(baseScheme);
        }
        else
        { baseExtent=null;
        }
      }
      else
      { baseExtent=null;
      }
    }  
    else
    { baseExtent=null;
    }
    
  }
  
  public ResultSetMapping(FieldSet fieldSet,LinkedTree<ResultColumnMapping> foldTree)
  { 
    this(fieldSet);
    applyFoldTree(foldTree);
  }

  private void applyFoldTree(LinkedTree<ResultColumnMapping> foldTree)
  {
    log.fine("Computing ResultSetMapping for "+fieldSet);

    if (baseExtent!=null)
    { baseExtent.applyFoldTree(foldTree);
    }
    
    if (foldTree!=null)
    {
      
      int baseColumnCount
        =(baseExtent!=null)
        ?baseExtent.getTotalColumnCount()
        :0
        ;
      int offset=baseColumnCount;
      int i=0;
      Iterator<? extends Field<?>> fieldIterator=fieldSet.fieldIterable().iterator();
      for (LinkedTree<ResultColumnMapping> node: foldTree)
      {        
        if (--offset>=0)
        { 
          log.fine("Skipping base extent field "+node.get());
          // Skip base extent nodes
          continue;
        }
        Field<?> field;
        if (!fieldIterator.hasNext())
        { break;
        }
        else
        { field=fieldIterator.next();
        }
        
        while ( (node.get()!=null 
                 && field!=null
                 && field!=node.get().columnMapping.getField()
                ) 
                
              )
        { 
          // Skip fields that are not in the db
          log.fine("Skipping non-persistent field "+field.getURI()+" map position "+field.getIndex());
          map[field.getIndex()]=null;
          subs[field.getIndex()]=null;
          if (fieldIterator.hasNext())
          { field=fieldIterator.next();
          }
          else 
          { field=null;
          }
        }
        if (field==null)
        { 
          log.fine("No more fields...done");
          break;
        }
        
        
        if (node.get()!=null)
        { 
          log.fine("Mapping["+field.getIndex()+"] "+field+" to "+node.get());
          map[field.getIndex()]=node.get();
        }
        if (!node.isLeaf())
        { 
          log.fine("SubMapping["+field.getIndex()+"] "+field);
          subs[field.getIndex()]
            =new ResultSetMapping
              (field.getType().getScheme()
              ,node
              );
        }
        i++;
        
      }
      totalColumnCount=baseColumnCount+i;
      log.fine("Column count is "+totalColumnCount);
    }
    else
    { defaultMap();
    }
  }

  /**
   * The number of fields contained in this and all base schemes
   * @return
   */
  int getTotalColumnCount()
  { return totalColumnCount;
  }
  
  private void defaultMap()
  {
    log.fine("Default mapping "+fieldSet);
    for (Field<?> field: fieldSet.fieldIterable())
    { 
      map[field.getIndex()]=new ResultColumnMapping(field.getIndex()+1);
      log.fine("Default Mapping["+field.getIndex()+"] "+field+" to "+map[field.getIndex()]);
    }
    totalColumnCount=fieldSet.getFieldCount();
  }
  
  
  
}