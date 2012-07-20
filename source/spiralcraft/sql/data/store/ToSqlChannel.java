package spiralcraft.sql.data.store;

import java.sql.SQLException;

import spiralcraft.lang.AccessException;
import spiralcraft.lang.Channel;
import spiralcraft.lang.reflect.BeanReflector;
import spiralcraft.lang.spi.SourcedChannel;
import spiralcraft.sql.converters.Converter;

public class ToSqlChannel<Tj,Ts>
  extends SourcedChannel<Tj,Ts>
{
  private final Converter<Ts,Tj> converter;
  
  public ToSqlChannel(Channel<Tj> source,Converter<Ts,Tj> converter)
  { 
    super(BeanReflector.<Ts>getInstance(converter.getSqlClass()),source);
    this.converter=converter;
  }

  @Override
  protected Ts retrieve()
  { 
    try
    { return converter.toSql(source.get());
    }
    catch (SQLException x)
    { throw new AccessException(x);
    }
  }

  @Override
  protected boolean store(Ts val)
  {
    try
    { return source.set(converter.fromSql(val));
    }
    catch (SQLException x)
    { throw new AccessException(x);
    }
  }
  
  @Override
  public boolean isWritable()
  { return source.isWritable();
  }
}
