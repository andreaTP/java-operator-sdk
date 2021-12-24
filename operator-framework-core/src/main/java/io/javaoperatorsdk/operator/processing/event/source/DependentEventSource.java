package io.javaoperatorsdk.operator.processing.event.source;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;

public interface DependentEventSource<P extends HasMetadata> extends EventSource {
  void initWith(EventSourceContext<P> context);
}
