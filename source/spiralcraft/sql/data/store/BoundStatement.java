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

import spiralcraft.lang.Channel;
import spiralcraft.lang.Expression;
import spiralcraft.lang.Focus;
import spiralcraft.lang.BindException;
import spiralcraft.log.ClassLog;
import spiralcraft.log.Level;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

import spiralcraft.data.DataException;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Field;

import spiralcraft.sql.SqlFragment;

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.dml.ValueExpression;
import spiralcraft.sql.dml.IdentifierChain;

import spiralcraft.util.Path;

import java.util.ArrayList;

/**
 * A PreparedStatement bound to a parameter context
 */
public abstract class BoundStatement
{
  protected final ClassLog log
    =ClassLog.getInstance(getClass());
  protected Level logLevel
    =ClassLog.getInitialDebugLevel(getClass(),Level.INFO);
  
  protected ArrayList<ParameterTag> parameterExpressions
    =new ArrayList<ParameterTag>();
  protected ArrayList<Converter<?,?>> parameterConverters
    =new ArrayList<Converter<?,?>>();
  protected ArrayList<Channel<?>> parameterBindings
    =new ArrayList<Channel<?>>();
  
  protected String statementText;
  protected SqlFragment sqlFragment;
  protected final SqlStore store;
  protected SqlBinding<?,?> statementBinding;
  protected FieldSet dataFields;
  @SuppressWarnings("rawtypes")
  protected Converter[] converters;
  protected TableMapping primaryTableMapping;
  
  /**
   * Create a new BoundStatement
   * 
   * @param store The SqlStore that provide the JDBC interfaces and
   *   Type/Column mappings
   *
   * @param dataFields The FieldSet, if any, which represents the data that
   *   will be read or written to the database (as opposed to other 
   *   parameters)
   */
  protected BoundStatement(SqlStore store,FieldSet dataFields)
  { 
    this.store=store;
    this.logLevel
      =store.getStatementLogLevel().canLog(logLevel)
      ?store.getStatementLogLevel()
      :logLevel
      ;
      
    this.dataFields=dataFields;
  }
  
  public SqlFragment getSqlFragment()
  { return sqlFragment;
  }
  
  /**
   * Generate the statement text and determine the parameter expressions
   */
  public void setSqlFragment(SqlFragment sqlFragment)
  { 
    parameterExpressions.clear();
    this.sqlFragment=sqlFragment;
    statementText=sqlFragment.generateSQL(parameterExpressions);
    if (logLevel.isFine())
    { log.fine(toString()+": Creating "+statementText);
    }
  }
  
  /**
   * <P>Bind the set of parameter Expressions to a new Focus (application data
   *   context)
   *   
   * <P>Called once when the caller has established the application context in
   *    which this statement will be executed, or when a different application
   *    context is to be used.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void bindParameters(Focus<?> focus)
    throws DataException
  { 
    try
    {
      
      parameterBindings.clear();
      for (ParameterTag tag: parameterExpressions)
      { 
        Expression<?> expression=tag.expression;
        
//        log.fine
//          ("BindParameters "+(parameterBindings.size()+1)+"= "
//          +expression.getText()
//          );
//        
//        expression.getRootNode().debugTree(System.err);
        
        Channel<?> channel=focus.bind(expression);
        parameterBindings.add(channel);
        parameterConverters.add(tag.converter);
      }
    }
    catch (BindException x)
    { 
      throw new DataException
        ("Error binding parameters for '"+statementText+"': "+x,x);
    }
    
  }
  
  /**
   * Get the Primary (unaliased) TableMapping associated with the
   *   FieldSet of this statement
   */
  public void setPrimaryTableMapping(TableMapping mapping)
  { primaryTableMapping=mapping;
  }
  
  
  public ColumnMapping getColumnMapping(Path fieldPath)
  {
    if (primaryTableMapping!=null)
    {
      return primaryTableMapping.getMappingForPath(fieldPath);
    }
    else
    { 
      throw new IllegalArgumentException
        ("No column mapping for "+fieldPath);
    }
    
    
  }
  
  /**
   * Map a Field name to a SQL ValueExpression in terms of the SQL objects this
   *   query is accessing.
   */
  public ValueExpression createColumnValueExpression(Path fieldPath)
  { 
//    System.err.println("BoundStatement: createColumnValueExpression: "+fieldPath);
    if (primaryTableMapping!=null)
    {
      ColumnMapping columnMapping=primaryTableMapping.getMappingForPath(fieldPath);
      if (columnMapping!=null && columnMapping.getColumnModel()!=null)
      { return columnMapping.getColumnModel().getValueExpression();
      }
      else
      { 
//        System.err.println("BoundStatement: createColumnValueExpression: "+columnMapping);
        return null;
      }
    }
    else if (dataFields!=null && fieldPath.size()==1)
    { 
      Field<?> field=dataFields.getFieldByName(fieldPath.getElement(0));
      if (field!=null)
      { return new IdentifierChain(field.getName());
      }
      else
      { return null;
      }
    }
    else if (fieldPath.size()==1)
    { return new IdentifierChain(fieldPath.getElement(0));
    }
    else
    { 
      throw new IllegalArgumentException
        ("Cannot translate Field path "+fieldPath
        +" to a SQL fragment"
        );
    }
  }
  
  /**
   * The parameters in native form to use for cache lookup
   * 
   * @return
   */
  public Object[] makeParameterKey()
  {
    Object[] key=new Object[parameterBindings.size()];
    int i=0;
    for (Channel<?> channel: parameterBindings)
    { key[i++]=channel.get();
    }
    return key;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object[] convertParameters(Object[] rawParameters)
    throws SQLException
  {
    Object[] params=new Object[rawParameters.length];
    int i=0;
    for (Converter converter: parameterConverters)
    { 
      params[i]
        =(converter!=null && rawParameters[i]!=null)
        ?converter.toSql(rawParameters[i])
        :rawParameters[i];
      i++;
    }
    return params;
  }
  
  protected String formatParameters(Object[] parameters)
  {
    StringBuilder buf=new StringBuilder();
    for (Object parameter: parameters)
    {
      if (parameter==null)
      { buf.append("[null]");
      }
      else
      { 
        buf
          .append("[")
          .append(parameter.toString())
          .append("]")
          .append(" ")
          .append(parameter.getClass().getName())
          .append("\r\n");
      }
    }
    return buf.toString();
  }
  
  /**
   * <P>Applies a new set of parameter values to a JDBC PreparedStatement.
   * 
   * <P>Called each time the statement is executed, immediately before
   *   execution
   */
  protected void applyParameters
    (PreparedStatement jdbcStatement,Object[] parameters)
    throws SQLException
  {
    jdbcStatement.clearParameters();
    parameters=convertParameters(parameters);
    int i=1;
    for (Object paramValue : parameters)
    { 
      try
      { jdbcStatement.setObject(i,paramValue);
      }
      catch (SQLSyntaxErrorException x)
      { 
        throw new SQLSyntaxErrorException
          ("Error assigning parameter "+i+" = "+paramValue
            +(paramValue!=null?("type="+paramValue.getClass().getName()):"")
            +" statement="+statementText
          );
      }
      i++;
    }
  }
  
}
