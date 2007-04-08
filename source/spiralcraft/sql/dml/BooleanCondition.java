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

package spiralcraft.sql.dml;

import spiralcraft.sql.SqlFragment;

public abstract class BooleanCondition
  extends SqlFragment
{
  
  public abstract int getPrecedence();

  public BooleanCondition isNull()
  { return new BooleanTest(this,false,new TruthValue());
  }
  
  public BooleanCondition isNotNull()
  { return new BooleanTest(this,true,new TruthValue());
  }
  
  public BooleanCondition not()
  { return new BooleanFactor(this);
  }
  
  public BooleanCondition or(BooleanCondition condition)
  { 
    if (condition.getPrecedence()>=getPrecedence())
    { 
      // We don't need parens, because the appended condition has higher or equal precedence
      //   than this condition.
      return new SearchCondition(this,condition);
    }
    else
    {
      // The appended condition has lower precedence than this condition 
      //   so we need to enclose it in parens to disambiguate
      return new SearchCondition(this,new BooleanPrimary(condition));
    }
  }
  
  public BooleanCondition and(BooleanCondition condition)
  { 
    if (condition.getPrecedence()>=getPrecedence())
    { 
      // We don't need parens, because the appended condition has higher or equal precedence
      //   than this condition.
      return new BooleanTerm(this,condition);
    }
    else
    {
      // The appended condition has lower precedence than this condition 
      //   so we need to enclose it in parens to disambiguate
      return new BooleanTerm(this,new BooleanPrimary(condition));
    }
  }
  
}
