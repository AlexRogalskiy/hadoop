package crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

public class CustomDoFn<S, T> extends DoFn<S, T> {

  static class NonSerializableHelper { }

  transient NonSerializableHelper helper;

  @Override
  public void initialize() {
    helper = new NonSerializableHelper();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(S input, Emitter<T> emitter) {
    // Tu użyj metod pomocniczych
    emitter.emit((T) input);
  }
}
