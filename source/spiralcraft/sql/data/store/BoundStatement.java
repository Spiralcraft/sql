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

import spiralcraft.lang.Optic;
import spiralcraft.lang.Expression;
import spiralcraft.lang.Focus;
import spiralcraft.lang.BindException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import spiralcraft.data.DataException;
import spiralcraft.data.FieldSet;
import spiralcraft.data.Field;

import spiralcraft.sql.SqlFragment;

import spiralcraft.sql.dml.ValueExpression;
import spiralcraft.sql.dml.IdentifierChain;

import spiralcraft.util.Path;

import java.util.ArrayList;

/**
 * A PreparedStatement bound to a parameter context
 */
public abstract class BoundStatement
{
  protected ArrayList<Expression> parameterExpressions
    =new ArrayList<Expression>();
  protected ArrayList<Optic> parameterBindings
    =new ArrayList<Optic>();
  
  protected String statementText;
  protected SqlFragment sqlFragment;
  protected final SqlStore store;
  protected SqlBinding statementBinding;
  protected FieldSet dataFields;
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
  }
  
  /**
   * <P>Bind the set of parameter Expressions to a new Focus (application data
   *   context)
   *   
   * <P>Called once when the caller has established the application context in
   *    which this statement will be executed, or when a different application
   *    context is to be used.
   */
  @SuppressWarnings("unchecked") // Use of Focus is not specifically typed
  public void bindParameters(Focus focus)
    throws DataException
  { 
    try
    {
      
      parameterBindings.clear();
      for (Expression expression: parameterExpressions)
      { 
        System.err.println
          ("BindParameters "+parameterBindings.size()+"= "
          +expression.getText()
          );
        
        expression.getRootNode().debugTree(System.err);
        
        parameterBindings.add(focus.bind(expression));
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
      Field field=dataFields.getFieldByName(fieldPath.getElement(0));
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
   * <P>Applies a new set of parameter values to a JDBC PreparedStatement.
   * 
   * <P>Called each time the statement is executed, immediately before
   *   execution
   */
  protected void applyParameters(PreparedStatement jdbcStatement)
    throws SQLException
  {
    jdbcStatement.clearParameters();
    
    int i=1;
    for (Optic optic: parameterBindings)
    { 
      Object paramValue=optic.get();
      System.err.println("Apply parameter "+i+" = "+paramValue);
      jdbcStatement.setObject(i++,paramValue);
    }
  }
  
}
