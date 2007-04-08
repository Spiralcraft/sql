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
package spiralcraft.sql.data.store;

import spiralcraft.data.Field;

import spiralcraft.sql.data.mappers.TypeMapper;
import spiralcraft.sql.dml.DerivedColumn;
import spiralcraft.sql.dml.SelectListItem;

import spiralcraft.sql.model.Column;


import spiralcraft.registry.Registrant;
import spiralcraft.registry.RegistryNode;

import java.util.ArrayList;

/**
 * An association between a Field in the Scheme of a Type and a Table column
 */
public class ColumnMapping
  implements Registrant
{
  private RegistryNode registryNode;
  
  private Field field;
  private String fieldName;
  private String columnName;
  private Column column;

  private boolean flatten;
  private ArrayList <ColumnMapping> flattenedChildren;
  
  public ColumnMapping()
  {
  }
  
  /**
   * Flatten a Type that has a scheme into this Table by
   *   using a column name prefix of columnName+"_"+subcolumn.columnName
   */
  public void setFlatten(boolean flatten)
  { this.flatten=flatten;
  }

  public void setField(Field field)
  { 
    this.field=field;
    fieldName=field.getName();
    columnName=field.getName();
  }
  
  public Field getField()
  { return field;
  }
  
  public String getFieldName()
  { return fieldName;
  }
  
  public void setFieldName(String fieldName)
  { this.fieldName=fieldName;
  }
  
  public String getColumnName()
  { return columnName;
  }
  
  public void setColumnName(String columnName)
  { this.columnName=columnName;
  }
  
  private void flatten()
  { 
    flattenedChildren=new ArrayList<ColumnMapping>
      (field.getType().getScheme().getFieldCount());
    
    for (Field subField : field.getType().getScheme().fieldIterable())
    { 
      ColumnMapping subMapping = new ColumnMapping();
      subMapping.setField(subField);
      subMapping.setFieldName(fieldName+"."+subMapping.getFieldName());
      subMapping.setColumnName(columnName+"_"+subMapping.getColumnName());
      flattenedChildren.add(subMapping);
      
    }
    
  }
  
  public void register(RegistryNode node)
  {
    System.err.println("ColumnMapping: "+fieldName+":"+columnName);
    node=node.createChild(fieldName);
    node.registerInstance(ColumnMapping.class,this);
    registryNode=node;
    if (flatten && field.getType().getScheme()!=null)
    { 
      flatten();
      for (ColumnMapping subMapping : flattenedChildren)
      { 
        subMapping.register(registryNode);
        
      }
    }

  }
  
  /**
   * 
   * @return The Column model associated with this mapping, if this mapping
   *   is directly associated with a column.
   */
  public Column getColumnModel()
  { return column;
  }
   
  @SuppressWarnings("unchecked") // Not using typeMapper in a generics way
  public Column[] getColumnModels()
  { 
    if (flattenedChildren!=null)
    { 
      ArrayList<Column> retList
        =new ArrayList<Column>();
      for (ColumnMapping mapping : flattenedChildren)
      { 
        for (Column column: mapping.getColumnModels()) 
        { retList.add(column);
        }
      }
      return retList.toArray(new Column[retList.size()]);
    }
    else if (columnName!=null)
    { 
      if (column==null)
      {
        TypeManager typeManager
          =(TypeManager) registryNode.findInstance
            (TypeManager.class);
        
        column=new Column();
        column.setName(columnName);

        TypeMapper typeMapper
          =typeManager.getTypeMapperForType(field.getType());
        if (typeMapper==null)
        { 
          System.err.println
            ("ColumnMapping.getColumnModels(): No mapper for type "
            +field.getType().getURI()
            );
          return new Column[0];
        }
            
        typeMapper.specifyColumn(field.getType(),column);
      }
      return new Column[] {column};
    }
    else
    { return new Column[0];
    }
    
  }

  
  /**
   * Create the set of SelectListItem SqlFragments that return the
   *   one or many values that make up this mapping.
   */
  public SelectListItem[] createSelectListItems()
  {
    if (flattenedChildren!=null)
    { 
      ArrayList<SelectListItem> retList
        =new ArrayList<SelectListItem>();
      for (ColumnMapping mapping : flattenedChildren)
      { 
        for (SelectListItem item: mapping.createSelectListItems()) 
        { retList.add(item);
        }
      }
      return retList.toArray(new SelectListItem[retList.size()]);
    }
    else if (columnName!=null)
    { 
      return new SelectListItem[]
        {new DerivedColumn
          (column.createValueExpression()
          )
        };
      
    }
    else
    { 
      // No actual field for this
      return new SelectListItem[0];
    }
  }
  
}
