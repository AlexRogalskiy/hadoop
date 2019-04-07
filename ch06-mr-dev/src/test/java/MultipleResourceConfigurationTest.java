// == MultipleResourceConfigurationTest
// == MultipleResourceConfigurationTest-Override
// == MultipleResourceConfigurationTest-Final
// == MultipleResourceConfigurationTest-Expansion
// == MultipleResourceConfigurationTest-SystemExpansion
// == MultipleResourceConfigurationTest-NoSystemByDefault
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class MultipleResourceConfigurationTest {
  
  @Test
  public void get() throws IOException {
	// Pojedynczy test do umieszczenia w książce
    
    // vv MultipleResourceConfigurationTest
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    conf.addResource("configuration-2.xml");
    // ^^ MultipleResourceConfigurationTest
    
    assertThat(conf.get("color"), is("yellow"));

    // Zmiana wartości
    // vv MultipleResourceConfigurationTest-Override
    assertThat(conf.getInt("size", 0), is(12));
    // ^^ MultipleResourceConfigurationTest-Override

    // Właściwości typu final nie można modyfikować
    // vv MultipleResourceConfigurationTest-Final
    assertThat(conf.get("weight"), is("heavy"));
    // ^^ MultipleResourceConfigurationTest-Final

    // Określanie wartości zmiennych
    // vv MultipleResourceConfigurationTest-Expansion
    assertThat(conf.get("size-weight"), is("12,heavy"));
    // ^^ MultipleResourceConfigurationTest-Expansion

    // Określanie wartości zmiennych dla właściwości systemowych
    // vv MultipleResourceConfigurationTest-SystemExpansion
    System.setProperty("size", "14");
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // ^^ MultipleResourceConfigurationTest-SystemExpansion

    // Właściwości systemowe nie są pobierane
    // vv MultipleResourceConfigurationTest-NoSystemByDefault
    System.setProperty("length", "2");
    assertThat(conf.get("length"), is((String) null));
    // ^^ MultipleResourceConfigurationTest-NoSystemByDefault

  }

}
