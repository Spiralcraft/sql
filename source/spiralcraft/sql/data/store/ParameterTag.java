package spiralcraft.sql.data.store;

import spiralcraft.lang.Expression;
import spiralcraft.sql.SqlType;
import spiralcraft.sql.converters.Converter;

public class ParameterTag
{
  final Expression<?> expression;
  final SqlType<?> sqlType;
  final Converter<?,?> converter;

  public ParameterTag
    (Expression<?> expression,SqlType<?> sqlType,Converter<?,?> converter)
  { 
    this.expression=expression;
    this.sqlType=sqlType;
    this.converter=converter;
  }
  
}
