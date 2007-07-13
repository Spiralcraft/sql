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
import spiralcraft.sql.dml.SetClause;
import spiralcraft.sql.dml.SqlParameterReference;
import spiralcraft.sql.dml.BooleanCondition;
import spiralcraft.sql.dml.ComparisonPredicate;

import spiralcraft.sql.model.Column;

import spiralcraft.registry.Registrant;
import spiralcraft.registry.RegistryNode;

import spiralcraft.lang.Expression;
import spiralcraft.lang.ParseException;

import spiralcraft.util.Path;
import spiralcraft.util.tree.LinkedTree;

import java.util.ArrayList;
import java.util.HashMap;


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
  private Path path;

  private boolean flatten;
  private ArrayList <ColumnMapping> flattenedChildren;
  private HashMap<String,ColumnMapping> childMapByField
    =new HashMap<String,ColumnMapping>();
  
  private SelectListItem selectListItem;
  private LinkedTree<ColumnMapping> columnMappings;
  private SetClause parameterizedSetClause;
  private SqlParameterReference parameterReference;
  private BooleanCondition parameterizedKeyCondition;
  
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
  
  public boolean isFlattened()
  { return flattenedChildren!=null;
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
  
  /**
   * Specify the name of the Field this ColumnMapping applies to.
   *   
   */
  public void setFieldName(String fieldName)
  { 
    this.fieldName=fieldName;
    if (this.field!=null && !fieldName.equals(field.getName()))
    { 
      throw new IllegalArgumentException
        ("Cannot change Field name '"+fieldName+"' for ColumnMapping");
    }
  }
  
  /**
   * Specify the Field Path (through nested Types) mapped by this Column. 
   */
  public void setPath(Path path)
  { this.path=path;
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
      subMapping.setColumnName(columnName+"_"+subMapping.getColumnName());
      subMapping.setPath(this.path.append(subField.getName()));
      flattenedChildren.add(subMapping);
      childMapByField.put(subMapping.getFieldName(), subMapping);
      
    }
    
  }
  
  ArrayList<ColumnMapping> getChildMappings()
  { return flattenedChildren;
  }
  
  ColumnMapping getChildMappingForPath(Path path)
  { 
    if (path.size()>0)
    {
      ColumnMapping mapping=childMapByField.get(path.getElement(0));
      if (mapping!=null && path.size()>1)
      { return mapping.getChildMappingForPath(path.subPath(1));
      }
      else
      { return mapping;
      }
    }
    return null;
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

  public synchronized SqlParameterReference getParameterReference()
  {
    if (parameterReference==null)
    {
      try
      {
        parameterReference
          =new SqlParameterReference(Expression.parse(path.format(".")));
        // System.err.println("ColumnMapping: "+columnName+" refs "+path.format("."));
      }
      catch (ParseException x)
      { throw new RuntimeException("Cannot parse "+path.format("."));
      }
    }
    return parameterReference;
  }
  

  public synchronized BooleanCondition getParameterizedKeyCondition()
  {
    if (parameterizedKeyCondition==null)
    { 
      if (flattenedChildren!=null)
      {
        // For complex Fields used as keys
        BooleanCondition condition=null;
        for (ColumnMapping mapping: flattenedChildren)
        {
          if (condition==null)
          { condition=mapping.getParameterizedKeyCondition();
          }
          else
          { condition=condition.and(mapping.getParameterizedKeyCondition());
          }
        }
        parameterizedKeyCondition=condition;
      }
      else
      { 
        parameterizedKeyCondition
          =new ComparisonPredicate
            (column.getValueExpression()
            ,"="
            ,getParameterReference()
            );
      }
    }
    return parameterizedKeyCondition;
  }
  
  public synchronized SetClause getParameterizedSetClause()
  { 
    if (parameterizedSetClause==null)
    {
      parameterizedSetClause=new SetClause
        (column.getName()
        ,getParameterReference()
        );
    }
    return parameterizedSetClause;
  }
  
  public SelectListItem getSelectListItem()
  { 
    if (selectListItem==null)
    { selectListItem=new DerivedColumn(column.getValueExpression());
    }
    return selectListItem;
    
  }
  
  /**
   * Create the set of SelectListItem SqlFragments that return the
   *   one or many values that make up this mapping.
   */
  public synchronized LinkedTree<ColumnMapping> getColumnMappings()
  {
    if (columnMappings==null)
    {  
      LinkedTree<ColumnMapping> node=new LinkedTree<ColumnMapping>(this);
      if (flattenedChildren!=null)
      { 
        for (ColumnMapping mapping : flattenedChildren)
        { node.addChild(mapping.getColumnMappings()); 
        }
      }
      columnMappings=node;
    }
    return columnMappings;
  }
  
}