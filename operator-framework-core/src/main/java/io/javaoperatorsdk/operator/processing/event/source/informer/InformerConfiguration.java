package io.javaoperatorsdk.operator.processing.event.source.informer;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.DefaultResourceConfiguration;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AssociatedSecondaryResourceIdentifier;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryResourcesRetriever;

public class InformerConfiguration<R extends HasMetadata, P extends HasMetadata>
    extends DefaultResourceConfiguration<R> {

  private final PrimaryResourcesRetriever<R> secondaryToPrimaryResourcesIdSet;
  private final AssociatedSecondaryResourceIdentifier<P> associatedWith;
  private final boolean skipUpdateEventPropagationIfNoChange;

  public InformerConfiguration(ConfigurationService service, String labelSelector,
      Class<R> resourceClass, String... namespaces) {
    this(service, labelSelector, resourceClass, Mappers.fromOwnerReference(), null, true,
        namespaces);
  }

  public InformerConfiguration(ConfigurationService service, String labelSelector,
      Class<R> resourceClass,
      PrimaryResourcesRetriever<R> secondaryToPrimaryResourcesIdSet,
      AssociatedSecondaryResourceIdentifier<P> associatedWith,
      boolean skipUpdateEventPropagationIfNoChange, String... namespaces) {
    this(service, labelSelector, resourceClass, secondaryToPrimaryResourcesIdSet, associatedWith,
        skipUpdateEventPropagationIfNoChange,
        namespaces != null ? Set.of(namespaces) : Collections.emptySet());
  }

  public InformerConfiguration(ConfigurationService service, String labelSelector,
      Class<R> resourceClass,
      PrimaryResourcesRetriever<R> secondaryToPrimaryResourcesIdSet,
      AssociatedSecondaryResourceIdentifier<P> associatedWith,
      boolean skipUpdateEventPropagationIfNoChange, Set<String> namespaces) {
    super(labelSelector, resourceClass, namespaces);
    setConfigurationService(service);
    this.secondaryToPrimaryResourcesIdSet =
        Objects.requireNonNullElse(secondaryToPrimaryResourcesIdSet, Mappers.fromOwnerReference());
    this.associatedWith =
        Objects.requireNonNullElseGet(associatedWith, () -> ResourceID::fromResource);
    this.skipUpdateEventPropagationIfNoChange = skipUpdateEventPropagationIfNoChange;
  }

  public PrimaryResourcesRetriever<R> getPrimaryResourcesRetriever() {
    return secondaryToPrimaryResourcesIdSet;
  }

  public AssociatedSecondaryResourceIdentifier<P> getAssociatedResourceIdentifier() {
    return associatedWith;
  }

  public boolean isSkipUpdateEventPropagationIfNoChange() {
    return skipUpdateEventPropagationIfNoChange;
  }
}
