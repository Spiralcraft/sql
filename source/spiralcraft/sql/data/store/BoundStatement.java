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

import spiralcraft.sql.SqlFragment;

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
  protected SqlFragment statement;
  protected final SqlStore store;
  
  protected BoundStatement(SqlStore store)
  { this.store=store;
  }
  
  /**
   * Generate the statement text and set up the parameter bindings
   */
  public void setStatement(SqlFragment statement)
  { 
    parameterExpressions.clear();
    this.statement=statement;
    statement.collectParameters(parameterExpressions);
    StringBuilder buffer=new StringBuilder();
    statement.write(buffer,"");
    statementText=buffer.toString();
  }
  
  @SuppressWarnings("unchecked") // Use of Focus is not specifically typed
  public void bindParameters(Focus focus)
    throws DataException
  { 
    try
    {
      parameterBindings.clear();
      for (Expression expression: parameterExpressions)
      { parameterBindings.add(focus.bind(expression));
      }
    }
    catch (BindException x)
    { 
      throw new DataException
        ("Error binding parameters for '"+statementText+"': "+x,x);
    }
    
  }
  
  protected void applyParameters(PreparedStatement jdbcStatement)
    throws SQLException
  {
    jdbcStatement.clearParameters();
    
    int i=1;
    for (Optic optic: parameterBindings)
    { jdbcStatement.setObject(i++,optic.get());
    }
  }
  
}
