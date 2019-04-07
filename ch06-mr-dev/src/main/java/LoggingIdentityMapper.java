//cc LoggingIdentityMapper Mapper tożsamościowy, który zapisuje dane do standardowego wyjścia i
// używa interfejsu API Apache Commons Logging
import java.io.IOException;

//vv LoggingIdentityMapper
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);
  
  @Override
  @SuppressWarnings("unchecked")
  public void map(KEYIN key, VALUEIN value, Context context)
      throws IOException, InterruptedException {
    // Wyświetlanie danych w standardowym wyjściu
    System.out.println("Klucz na etapie mapowania: " + key);
    
    // Rejestrowanie danych w dzienniku systemowym
    LOG.info("Klucz na etapie mapowania: " + key);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Wartość na etapie mapowania: " + value);
    }
    context.write((KEYOUT) key, (VALUEOUT) value);
  }
}
//^^ LoggingIdentityMapper