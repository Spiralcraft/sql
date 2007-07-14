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

import java.util.List;

import spiralcraft.lang.Focus;
import spiralcraft.lang.Expression;

import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;

import spiralcraft.data.query.BoundQuery;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.Query;

import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.CriteriaTranslator;
import spiralcraft.sql.data.store.SqlStore;

import spiralcraft.sql.dml.SelectStatement;

/**
 * A SQL optimized implementation of the Selection Query. 
 */
public class BoundSelection<Tt extends Tuple>
  extends BoundSqlQuery<Selection,Tt>
{
  
  private final BoundQuery<?,?> source;
  private Expression<Boolean> criteria;
  private Expression<Boolean> remainderCriteria;

  
  public BoundSelection(Selection selection,Focus<?> parentFocus,SqlStore store)
    throws DataException
  { 
    super(selection,parentFocus,store);
    
    this.criteria=selection.getConstraints();
    List<Query> sources=selection.getSources();
    if (sources.size()<1)
    { throw new DataException(getClass().getName()+": No source to bind to");
    }
    
    if (sources.size()>1)
    { throw new DataException(getClass().getName()+": Can't bind to more than one source");
    }

    this.source=store.query(sources.get(0),parentFocus);
  }
    
  public Expression<Boolean> getRemainderCriteria()
  { return remainderCriteria;
  }
  
  /**
   * Compose the Select by adding to the TypeAccess
   */
  public BoundQueryStatement composeStatement()
    throws DataException
  {
    if (source instanceof BoundScan)
    {
      BoundQueryStatement statement
        =((BoundScan<?>) source).composeStatement();

      SelectStatement select=(SelectStatement) statement.getSqlFragment();
      
      if (criteria!=null)
      {
        System.err.println("BoundSelection: Criteria = "+criteria);
        CriteriaTranslator translator
          =new CriteriaTranslator(criteria,statement);

        if (translator.getWhereClause()!=null)
        { select.setWhereClause(translator.getWhereClause());
        }
        remainderCriteria=translator.getFilterExpression();

        statement.setSqlFragment(select);
      }
      
      return statement;
    }
    else
    { throw new DataException("Cannot SQL Select from anything but a TypeAccess source");
    }
      
    
  }
  

}