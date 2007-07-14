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
 * A SQL boolean test or "IS" expression
 *
 */
public class BooleanTest
  extends BooleanCondition
{
  private BooleanCondition booleanPrimary;
  private boolean not;
  private TruthValue truthValue;
  
  public BooleanTest(BooleanCondition booleanPrimary,boolean not,TruthValue truthValue)
  { 
    this.booleanPrimary=booleanPrimary;
    this.not=not;
    this.truthValue=truthValue;
  }
  
  public void write(StringBuilder buffer,String indent, List<?> parameterCollector)
  {
    booleanPrimary.write(buffer,indent, parameterCollector);
    buffer.append(" IS ");
    if (not)
    { buffer.append(" NOT ");
    }
    truthValue.write(buffer,indent, parameterCollector);
  }
  
  public int getPrecedence()
  { return 4;
  }
}
