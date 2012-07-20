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

import spiralcraft.lang.Expression;

import spiralcraft.lang.parser.Node;
import spiralcraft.lang.parser.LogicalAndNode;
import spiralcraft.lang.parser.LogicalOrNode;
import spiralcraft.lang.parser.EqualityNode;
import spiralcraft.lang.parser.RelationalNode;
import spiralcraft.lang.parser.ContextIdentifierNode;
import spiralcraft.lang.parser.CurrentFocusNode;


import spiralcraft.sql.SqlFragment;

import spiralcraft.sql.dml.WhereClause;
import spiralcraft.sql.dml.BooleanCondition;
import spiralcraft.sql.dml.ValueExpression;
import spiralcraft.sql.dml.SqlParameterReference;

import spiralcraft.util.Path;

/**
 * <P>Translates an Expression into a SQL "WHERE" clause to the maximum
 *   extent possible.
 * 
 * <P>The result will be the filter Expression, which consists of
 *   untranslatable logic at higher levels of the Expression parse tree which
 *   will post-filter the result, and a WHERE clause SqlFragment with
 *   parameter references to lower levels of the Expression parse tree,
 *   which will generate parameter values pre-execution.
 */
public class CriteriaTranslator
{

  private Expression<Boolean> filterExpression;
  private WhereClause whereClause;
  private BoundStatement statementContext;
  
  public CriteriaTranslator
    (Expression<Boolean> criteria
    ,BoundStatement statementContext
    )
  {
    this.statementContext=statementContext;
    Node root=criteria.getRootNode();
    Translation<BooleanCondition> result=translateBooleanCondition(root);
    if (result.sql!=null)
    { whereClause=new WhereClause(result.sql);
    }
    if (result.remainder!=null)
    { filterExpression=Expression.<Boolean>create(result.remainder);
    }
    
  }
  
  public Expression<Boolean> getFilterExpression()
  { return filterExpression;
  }
  
  public WhereClause getWhereClause()
  { return whereClause;
  }
  
  @SuppressWarnings("rawtypes")
  private Translation<BooleanCondition> translateBooleanCondition(Node node)
  { 
    if (node instanceof LogicalAndNode)
    { return translateAnd((LogicalAndNode) node);
    }
    else if (node instanceof LogicalOrNode)
    { return translateOr((LogicalOrNode) node);
    }
    else if (node instanceof EqualityNode)
    { return translateEquals((EqualityNode) node);
    }
    else if (node instanceof RelationalNode)
    { return translateRelational((RelationalNode) node);
    }
    else
    {
      Translation<BooleanCondition> result=new Translation<BooleanCondition>();
      result.remainder=node;
      return result;
    }
  }
  
  private Translation<ValueExpression> translateValueExpression(Node node)
  {

    if (node instanceof ContextIdentifierNode)
    { return translateContextIdentifier((ContextIdentifierNode) node);
    }
    else
    { 
      // Anything we don't recognize becomes a parameter
      return translateToParameter(node);
    }
    // TODO: There are other options- deal with ResolveNode,
    //   especially the case where the Field Type is a Tuple that
    //   is 'rolled up' into the parent table- ie. address.city
  }
  
  private Translation<ValueExpression>
    translateContextIdentifier(ContextIdentifierNode node)
  { 
    // Determine if this refers to a name on the server side or on the client 
    //   side.
    
    Translation<ValueExpression> translation=new Translation<ValueExpression>();
    
    if (node.getSource()==null || node.getSource() instanceof CurrentFocusNode)
    { 
      // The name refers to the local context. Map it to a column.
      

      ValueExpression expr
        =statementContext.createColumnValueExpression
          (new Path(new String[] {node.getIdentifier()},false));
      if (expr!=null)
      { 
        // We are referencing a DB field
        translation.sql=expr;
      }
      else
      { 
        // We are referencing a derived field not in the DB
        translation.remainder=node;
      }
    }
    else
    { 
      // A source in something other than the current context. Parameterize it.
      translation.sql=new SqlParameterReference<ParameterTag>
        (new ParameterTag
          (Expression.<Object>create(node)
           ,null // Unknown target type
           ,null
          )
        );
    }
    return translation;
  }

  private Translation<ValueExpression> translateToParameter(Node node)
  { 
    Translation<ValueExpression> translation=new Translation<ValueExpression>();
    translation.sql=new SqlParameterReference<ParameterTag>
      (new ParameterTag
        (Expression.<Object>create(node)
        ,null
        ,null
        )
      );
    return translation;
  }
  
  
  @SuppressWarnings("rawtypes")
  private Translation<BooleanCondition> translateRelational(RelationalNode node)
  {
    Node lhs=node.getLeftOperand();
    Node rhs=node.getRightOperand();

    Translation<BooleanCondition> result=new Translation<BooleanCondition>();
    
    Translation<ValueExpression> ltrans=translateValueExpression(lhs);
    Translation<ValueExpression> rtrans=translateValueExpression(rhs);
    
    // Conditional must have both sides translated and at least one side not
    //   parameterized, or evaluation must  take place locally
    if (ltrans.sql!=null 
        && rtrans.sql!=null 
        && ltrans.remainder==null 
        && rtrans.remainder==null
        && !(ltrans.sql instanceof SqlParameterReference
            && rtrans.sql instanceof SqlParameterReference
            )
        )
    { 
      if (node.isGreaterThan())
      { 
        if (node.isEqual())
        { result.sql=ltrans.sql.isGreaterThanOrEqual(rtrans.sql);
        }
        else
        { result.sql=ltrans.sql.isGreaterThan(rtrans.sql);
        }
      }
      else
      { 
        if (node.isEqual())
        { result.sql=ltrans.sql.isLessThanOrEqual(rtrans.sql);
        }
        else
        { result.sql=ltrans.sql.isLessThan(rtrans.sql);
        }
      }
    }
    else
    { result.remainder=node;
    }
    return result;
  }
  
  @SuppressWarnings("rawtypes")
  private Translation<BooleanCondition> translateEquals(EqualityNode node)
  {
    Node lhs=node.getLeftOperand();
    Node rhs=node.getRightOperand();

    Translation<BooleanCondition> result=new Translation<BooleanCondition>();
    
    Translation<ValueExpression> ltrans=translateValueExpression(lhs);
    Translation<ValueExpression> rtrans=translateValueExpression(rhs);
    
    // Conditional must have both sides translated, or evaluation must
    //   take place locally
    if (ltrans.sql!=null 
        && rtrans.sql!=null 
        && ltrans.remainder==null 
        && rtrans.remainder==null
        && !(ltrans.sql instanceof SqlParameterReference
            && rtrans.sql instanceof SqlParameterReference
            )
       )
    { 
      if (node.isNegated())
      { result.sql=ltrans.sql.isNotEqual(rtrans.sql);
      }
      else
      { result.sql=ltrans.sql.isEqual(rtrans.sql);
      }
    }
    else
    { result.remainder=node;
    }
    return result;
  }

  private Translation<BooleanCondition> translateOr(LogicalOrNode node)
  {
    Node lhs=node.getLeftOperand();
    Node rhs=node.getRightOperand();

    Translation<BooleanCondition> result=new Translation<BooleanCondition>();
    
    Translation<BooleanCondition> ltrans=translateBooleanCondition(lhs);
    Translation<BooleanCondition> rtrans=translateBooleanCondition(rhs);
    
    // OR must have both sides fully translated, or it must return
    //   all results back to the client
    if (ltrans.sql!=null 
       && rtrans.sql!=null
       && ltrans.remainder==null
       && rtrans.remainder==null
       )
    { result.sql=ltrans.sql.or(rtrans.sql);
    }
    else
    { result.remainder=node;
    }
    
    return result;
  }

  private Translation<BooleanCondition> translateAnd(LogicalAndNode node)
  {
    Node lhs=node.getLeftOperand();
    Node rhs=node.getRightOperand();

    Translation<BooleanCondition> result=new Translation<BooleanCondition>();
    
    Translation<BooleanCondition> ltrans=translateBooleanCondition(lhs);
    Translation<BooleanCondition> rtrans=translateBooleanCondition(rhs);
    
    // AND can have either side translated. The non-translated side will
    //   run on the client
    
    if (ltrans.sql!=null)
    {
      if (rtrans.sql!=null)
      { result.sql=ltrans.sql.and(rtrans.sql);
      }
      else
      { result.sql=ltrans.sql;
      }
    }
    else if (rtrans.sql!=null)
    { result.sql=rtrans.sql;
    }
    
    if (ltrans.remainder!=null)
    { 
      if (rtrans.remainder!=null)
      { result.remainder=ltrans.remainder.and(rtrans.remainder);
      }
      else
      { result.remainder=ltrans.remainder;
      }
    }
    else if (rtrans.remainder!=null)
    { result.remainder=rtrans.remainder;
    }
    
    return result;
  }
}

class Translation<T extends SqlFragment>
{
  public T sql;
  public Node remainder;
}
