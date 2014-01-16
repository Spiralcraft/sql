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

import spiralcraft.lang.BindException;
import spiralcraft.lang.Focus;
import spiralcraft.lang.Expression;
import spiralcraft.lang.util.ContextualFilter;
import spiralcraft.data.DataException;
import spiralcraft.data.Tuple;
import spiralcraft.data.access.SerialCursor;
import spiralcraft.data.access.kit.FilteringSerialCursor;
import spiralcraft.data.lang.DataReflector;
import spiralcraft.data.query.Selection;
import spiralcraft.data.query.Query;
import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.CriteriaTranslator;
import spiralcraft.sql.data.store.SqlStore;
import spiralcraft.sql.data.store.TableMapping;
import spiralcraft.sql.dml.SelectStatement;

/**
 * A SQL optimized implementation of the Selection Query. 
 */
public class BoundSelection
  extends BoundSqlQuery<Selection>
{
  
  private Expression<Boolean> criteria;
  private Expression<Boolean> remainderCriteria;
  private ContextualFilter<Tuple> filter;

  
  public BoundSelection
    (Selection selection
    ,Focus<?> parentFocus
    ,SqlStore store
    ,TableMapping mapping
    )
    throws DataException
  { 
    super(selection,parentFocus,store,mapping);
    this.criteria=selection.getConstraints();
    List<Query> sources=selection.getSources();
    if (sources.size()<1)
    { throw new DataException(getClass().getName()+": No source to bind to");
    }
    
    if (sources.size()>1)
    { throw new DataException(getClass().getName()+": Can't bind to more than one source");
    }

    // this.source=store.query(sources.get(0),parentFocus);
  }
    
  /**
   * The subtree of the supplied criteria that could not be executed on the
   *   server, and must be processed locally.
   *   
   * @return
   */
  public Expression<Boolean> getRemainderCriteria()
  { return remainderCriteria;
  }
  
  /**
   * Compose the Select by adding to the TypeAccess
   */
  @Override
  protected BoundQueryStatement composeStatement()
    throws DataException
  {
    if (debugLevel.isFine())
    { log.fine("Composing "+this+" "+getQuery());
    }
    BoundQueryStatement statement
      =new BoundQueryStatement(store,mapping.getType().getFieldSet());

    statement.setPrimaryTableMapping(mapping);
    
    SelectStatement select=new SelectStatement();
    select.setFromClause(mapping.getFromClause());

    select.setSelectList(mapping.getSelectList());
  
    statement.setResultSetMapping(mapping.getResultSetMapping());
    
    
    if (criteria!=null)
    {
      if (debugLevel.isFine())
      { log.fine("BoundSelection: Criteria = "+criteria);
      }
      CriteriaTranslator translator
        =new CriteriaTranslator(criteria,statement);

      if (translator.getWhereClause()!=null)
      { 
        select.setWhereClause(translator.getWhereClause());
        if (debugLevel.isFine())
        { log.fine("WHERE clause = "+translator.getWhereClause());
        }
      }
      remainderCriteria=translator.getFilterExpression();
      if (remainderCriteria!=null)
      {
        if (debugLevel.isFine())
        { log.fine("Post filter = "+remainderCriteria);
        }
        try
        {
          filter
            =new ContextualFilter<Tuple>
              (this.paramFocus
              ,remainderCriteria
              ,DataReflector.<Tuple>getInstance(mapping.getType())
              );
        }
        catch (BindException x)
        { throw new DataException("Error binding filter "+remainderCriteria,x);
        }
      }
    }
    
    statement.setSqlFragment(select);
    return statement;
      
    
  }
  
  @Override
  protected SerialCursor<Tuple> doExecute()
    throws DataException
  { 
    SerialCursor<Tuple> result=super.doExecute();
    
    if (filter==null)
    { return result;
    }
    else
    { return new FilteringSerialCursor<Tuple>(result,filter);
    }
  }  

}