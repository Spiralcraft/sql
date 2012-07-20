package spiralcraft.sql.data;

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.data.store.ColumnMapping;

public class ResultMapping
{
  
  public final int resultSetColumn;
  @SuppressWarnings("rawtypes")
  public final Converter converter;
  public final ColumnMapping columnMapping;
  
  public ResultMapping(int resultSetColumn)
  { 
    this.resultSetColumn=resultSetColumn;
    this.converter=null;
    this.columnMapping=null;
  }
  
  public ResultMapping(int resultSetColumn,ColumnMapping column)
  { 
    this.resultSetColumn=resultSetColumn;
    this.converter=column.getConverter();
    this.columnMapping=column;
  }
  
  @Override
  public String toString()
  { 
    return super.toString()
      +"[resultSetColumn="+resultSetColumn
      +", converter="+converter
      +", columnMapping="+columnMapping.getColumnName()+"->"+columnMapping.getFieldName()
      +"]"
      ;
  }
}
