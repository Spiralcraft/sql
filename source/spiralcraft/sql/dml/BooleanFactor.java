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

/**
 * 
 * A SQL Boolean Factor or "NOT" expression
 *
 */
public class BooleanFactor
  extends BooleanCondition
{
  private boolean not;
  private BooleanCondition booleanTest;
  
  public BooleanFactor(BooleanCondition booleanTest)
  { 
    this.not=true;
    this.booleanTest=booleanTest;
  }
  
  @Override
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    if (not)
    { buffer.append(" NOT ");
    }
    booleanTest.write(buffer,indent, parameterCollector);
  }
  
  @Override
  public int getPrecedence()
  { return 3;
  }
  
  @Override
  public String toString()
  { 
    if (not)
    { return " NOT "+booleanTest;
    }
    else
    { return booleanTest.toString();
    }
  }

}
