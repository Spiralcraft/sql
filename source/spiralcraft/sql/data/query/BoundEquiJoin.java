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

import java.util.ArrayList;
import java.util.List;

import spiralcraft.lang.Focus;
import spiralcraft.lang.Expression;
import spiralcraft.lang.parser.ResolveNode;
import spiralcraft.lang.parser.ContextIdentifierNode;

import spiralcraft.data.DataException;
import spiralcraft.data.KeyTuple;
import spiralcraft.data.Projection;
import spiralcraft.data.Tuple;

import spiralcraft.data.access.SerialCursor;
import spiralcraft.data.access.cache.CacheIndex;
import spiralcraft.data.access.cache.KeyedDataProvider;
import spiralcraft.data.query.EquiJoin;
import spiralcraft.data.query.Query;

import spiralcraft.sql.data.SerialResultSetCursor;
import spiralcraft.sql.data.store.BoundQueryStatement;
import spiralcraft.sql.data.store.ParameterTag;
import spiralcraft.sql.data.store.SqlStore;
import spiralcraft.sql.data.store.TableMapping;

import spiralcraft.sql.dml.BooleanCondition;
import spiralcraft.sql.dml.SelectStatement;
import spiralcraft.sql.dml.SqlParameterReference;
import spiralcraft.sql.dml.ValueExpression;
import spiralcraft.sql.dml.WhereClause;
import spiralcraft.util.Path;

/**
 * A SQL optimized implementation of the EquiJoin Query. 
 */
public class BoundEquiJoin
  extends BoundSqlQuery<EquiJoin>
  implements KeyedDataProvider
{
  
  private Projection<Tuple> projection;
  private CacheIndex cacheIndex;
  
  public BoundEquiJoin
    (EquiJoin query,Focus<?> parentFocus,SqlStore store,TableMapping mapping)
    throws DataException
  { 
    super(query,parentFocus,store,mapping);
    
    List<Query> sources=query.getSources();
    if (sources.size()<1)
    { throw new DataException(getClass().getName()+": No source to bind to");
    }
    
    if (sources.size()>1)
    { throw new DataException(getClass().getName()+": Can't bind to more than one source");
    }

    ArrayList<Expression<?>> lhsExpressions=query.getLHSExpressions();

      // TODO: Check keys here: This should really be 
      //   getResultType().getProjection
      //       (lhsExpressions.toArray(new Expression[0]))
      
    projection
      =mapping.getType().getScheme().getProjection
        (lhsExpressions.toArray(new Expression<?>[0]));
    cacheIndex=mapping.getCache().getIndex(projection);
    
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
    
    int i=0;
    BooleanCondition fullCondition=null;
    for (Expression<?> expr: getQuery().getLHSExpressions())
    { 
      String identifierName
        =(expr.getRootNode() instanceof ResolveNode)
        ?((ResolveNode<?>) expr.getRootNode()).getIdentifierName()
        :((ContextIdentifierNode) expr.getRootNode()).getIdentifier()
        ;
      Path fieldPath=new Path(new String[] {identifierName},false);
      ValueExpression lhs=statement.createColumnValueExpression
        (fieldPath);
      
      SqlParameterReference<?> rhs
        =new SqlParameterReference<ParameterTag>
          (new ParameterTag
            (getQuery().getRHSExpressions().get(i)
            ,statement.getColumnMapping(fieldPath).getColumnModel().getType()
            ,statement.getColumnMapping(fieldPath).getConverter()
            )
          );
      
      BooleanCondition lhsEqualsRhs = lhs.isEqual(rhs);
      
      if (fullCondition==null)
      { fullCondition=lhsEqualsRhs;
      }
      else
      { fullCondition=fullCondition.and(lhsEqualsRhs);
      }
      
      i++;
    }
    
    if (fullCondition!=null)
    { select.setWhereClause(new WhereClause(fullCondition));
    }

    statement.setSqlFragment(select);
    return statement;
      
    
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  protected SerialCursor<Tuple> doExecute()
    throws DataException
  { 
    
    Object[] parameterKey=statement.makeParameterKey();
    
    if (cacheIndex!=null)
    {
      return (SerialCursor) cacheIndex.fetch
        (new KeyTuple(projection,parameterKey,true)
        ,this
        );
    }
    else
    {
      // Return a cache cursor
      SerialResultSetCursor cursor
          =statement.execute(parameterKey); 
      return mapping.getCache().cache(cursor);
    }
  }
  
  @Override
  public SerialCursor<Tuple> fetch(KeyTuple tuple)
    throws DataException
  {
    return statement.execute(tuple.getData());
  }
  
}