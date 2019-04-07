// cc RegexExcludePathFilter Klasa z rodziny PathFilter służąca do wykluczania ścieżek 
// pasujących do danego wyrażenia regularnego
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

// vv RegexExcludePathFilter
public class RegexExcludePathFilter implements PathFilter {
  
  private final String regex;

  public RegexExcludePathFilter(String regex) {
    this.regex = regex;
  }

  public boolean accept(Path path) {
    return !path.toString().matches(regex);
  }
}
// ^^ RegexExcludePathFilter
