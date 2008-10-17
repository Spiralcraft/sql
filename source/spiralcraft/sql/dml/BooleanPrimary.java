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

import java.util.List;

public class BooleanPrimary
  extends BooleanCondition
{
  private Predicate predicate;
  private BooleanCondition booleanCondition;
  
  public BooleanPrimary(Predicate predicate)
  { this.predicate=predicate;
  }
  
  public BooleanPrimary(BooleanCondition booleanCondition)
  { this.booleanCondition=booleanCondition;
  }
  
  @Override
  public int getPrecedence()
  { return 5;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    if (predicate!=null)
    { 
      buffer.append("\r\n").append(indent);
      predicate.write(buffer,indent, parameterCollector);
    }
    else
    { 
      buffer.append("\r\n").append(indent).append("(");
      indent=indent+"  ";
      booleanCondition.write(buffer,indent, parameterCollector);
      buffer.append("\r\n").append(indent).append(")");
    }
   
  }
}
