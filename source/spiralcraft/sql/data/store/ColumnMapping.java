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

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.data.mappers.TypeMapper;

import spiralcraft.sql.dml.DerivedColumn;
import spiralcraft.sql.dml.SelectListItem;
import spiralcraft.sql.dml.SetClause;
import spiralcraft.sql.dml.SqlParameterReference;
import spiralcraft.sql.dml.BooleanCondition;
import spiralcraft.sql.dml.ComparisonPredicate;

import spiralcraft.sql.model.Column;


import spiralcraft.lang.Expression;
import spiralcraft.lang.ParseException;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;

import spiralcraft.util.Path;
import spiralcraft.util.tree.LinkedTree;

import java.util.ArrayList;
import java.util.HashMap;

import spiralcraft.data.Type;


/**
 * An association between a Field in the Scheme of a Type and a Table column
 */
public class ColumnMapping
{
  private static final ClassLog log=ClassLog.getInstance(ColumnMapping.class);
  private static final Level debugLevel
    =ClassLog.getInitialDebugLevel(ColumnMapping.class, null);
  
  
  private Field<?> field;
  private String fieldName;
  private String columnName;
  private Column column;
  private Path path;
  private Converter<?,?> converter;

  private boolean flatten;
  private ArrayList <ColumnMapping> flattenedChildren;
  private HashMap<String,ColumnMapping> childMapByField
    =new HashMap<String,ColumnMapping>();
  
  private SelectListItem selectListItem;
  private LinkedTree<ColumnMapping> columnMappings;
  private SetClause parameterizedSetClause;
  private SqlParameterReference<ParameterTag> parameterReference;
  private BooleanCondition parameterizedKeyCondition;
  private SqlStore store;
  private boolean resolved;
  
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

  public void setField(Field<?> field)
  { 
    this.field=field;
    fieldName=field.getName();
    columnName=field.getName();
  }
  
  public Field<?> getField()
  { return field;
  }
  
  public String getFieldName()
  { return fieldName;
  }
  
  public Converter<?,?> getConverter()
  { return converter;
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
    
    for (Field<?> subField : field.getType().getFieldSet().fieldIterable())
    { 
      if (TypeManager.isPersistent(field.getType(),subField))
      {
        ColumnMapping subMapping = new ColumnMapping();
        subMapping.setField(subField);
        subMapping.setColumnName(columnName+"_"+subMapping.getColumnName());
        subMapping.setPath(this.path.append(subField.getName()));
        flattenedChildren.add(subMapping);
        childMapByField.put(subMapping.getFieldName(), subMapping);
        // log.fine(""+field.getURI()+" -> "+subMapping);
      }
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
  
  public void resolve()
  {
    if (debugLevel.canLog(Level.DEBUG))
    { log.debug("ColumnMapping: "+fieldName+":"+columnName);
    }

    if (!field.isTransient() 
        && !field.getType().isPrimitive() 
        && (field.getType().getNativeClass()==null 
             || !Type.class.isAssignableFrom(field.getType().getNativeClass())
           )
       
        )
    { 
      if (field.getType().getScheme()!=null)
      {
        flatten=true;
        if (debugLevel.isDebug())
        { log.fine("Will flatten "+field.getURI());
        }
      }
      else
      { 
        if (debugLevel.isDebug())
        { log.fine("Scheme is null for "+field.getType());
        }
      }
    }
    
    if (flatten && field.getType().getScheme()!=null)
    { 
      if (debugLevel.isDebug())
      { log.fine("Flattening "+field.getURI());
      }
      
      flatten();
      
      ArrayList<ColumnMapping> missingColumns
        =new ArrayList<ColumnMapping>();
      
      for (ColumnMapping subMapping : flattenedChildren)
      { 
        subMapping.setStore(store);
        subMapping.resolve();
        if (subMapping.getColumnModel()==null
            && (subMapping.getColumnModels()==null
                || subMapping.getColumnModels().length==0
                )
           )
        { missingColumns.add(subMapping);
        }
            
        // log.fine("Flattened to "+subMapping);
        
      }
      for (ColumnMapping mc : missingColumns)
      { flattenedChildren.remove(mc);
      }
    }
    resolved=true;
  }
  
  /**
   * 
   * @return The Column model associated with this mapping, if this mapping
   *   is directly associated with a column.
   */
  public Column getColumnModel()
  { return column;
  }
  
  void setStore(SqlStore store)
  { this.store=store;
  }
   
  @SuppressWarnings({ "unchecked", "rawtypes" }) // Not using typeMapper in a generics way
  public Column[] getColumnModels()
  { 
    if (!resolved)
    { throw new IllegalStateException("Not resolved");
    }
    if (flattenedChildren!=null)
    { 
      ArrayList<Column> retList
        =new ArrayList<Column>();
      for (ColumnMapping mapping : flattenedChildren)
      { 
        // log.fine("Expanding "+mapping);
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

        
        Column column=new Column();
        column.setName(columnName);

        TypeMapper typeMapper
          =store.getTypeManager().getTypeMapperForType(field.getType());
        if (typeMapper==null)
        { 
          if (debugLevel.canLog(Level.DEBUG))
          {
            log.debug
              ("ColumnMapping.getColumnModels(): No mapper for type "
              +field.getType().getURI()
              +" ("+field.getType().getClass().getName()+")"
              );
          }
          return new Column[0];
        }

            
        typeMapper.specifyColumn(field.getType(),column);
        if (column.getType()==null)
        { 
          log.warning
            ("Mapper "+typeMapper+" did not map type "+field.getType().getURI()
            +" for column "+column.getName()
            );
          return new Column[0];
          
        }
        converter=typeMapper.getConverter(field.getType());
        this.column=column;
      }
      return new Column[] {column};
    }
    else
    { return new Column[0];
    }
    
  }

  public synchronized SqlParameterReference<ParameterTag> getParameterReference()
  {
    if (parameterReference==null)
    {
      try
      {
        parameterReference
          =new SqlParameterReference<ParameterTag>
            (new ParameterTag
              (Expression.parse(path.format("."))
              ,column.getType()
              ,converter
              )
            );
        
        if (debugLevel.canLog(Level.DEBUG))
        { log.debug("ColumnMapping: "+columnName+" refs "+path.format("."));
        }
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
    if (column==null)
    { throw new IllegalStateException("Column is null for "+field.getURI());
    }
    
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
  
  @Override
  public String toString()
  { 
    return super.toString()
     +"[field="+field.getURI()+", columnName="+columnName+"]"
     +(columnMappings!=null?columnMappings.toString():"");
  }
  
}
