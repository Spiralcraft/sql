package spiralcraft.sql.data;

import spiralcraft.sql.converters.Converter;
import spiralcraft.sql.data.store.ColumnMapping;

public class ResultColumnMapping
{
  
  public final int resultSetColumn;
  @SuppressWarnings("rawtypes")
  public final Converter converter;
  public final ColumnMapping columnMapping;
  
  public ResultColumnMapping(int resultSetColumn)
  { 
    this.resultSetColumn=resultSetColumn;
    this.converter=null;
    this.columnMapping=null;
  }
  
  public ResultColumnMapping(int resultSetColumn,ColumnMapping column)
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
      +", columnMapping="
        +(columnMapping!=null?(columnMapping.getColumnName()+"->"+columnMapping.getFieldName()):"NULL")
      +"]"
      ;
  }
}
