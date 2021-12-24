package io.javaoperatorsdk.operator.processing;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.CustomResourceUtils;
import io.javaoperatorsdk.operator.MissingCRDException;
import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.api.config.BaseConfigurationService;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.config.DependentResource;
import io.javaoperatorsdk.operator.api.config.Version;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.monitoring.Metrics.ControllerExecution;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ContextInitializer;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContextInjector;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResourceController;
import io.javaoperatorsdk.operator.api.reconciler.dependent.KubernetesDependentResourceController;
import io.javaoperatorsdk.operator.processing.event.EventSourceManager;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Controller<R extends HasMetadata> implements Reconciler<R>,
    LifecycleAware, EventSourceInitializer<R> {

  private static final Logger log = LoggerFactory.getLogger(Controller.class);

  private final Reconciler<R> reconciler;
  private final ControllerConfiguration<R> configuration;
  private final KubernetesClient kubernetesClient;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private List<DependentResourceController> dependents;
  private final EventSourceManager<R> eventSourceManager;
  private ConfigurationService configurationService;

  public Controller(Reconciler<R> reconciler,
      ControllerConfiguration<R> configuration,
      KubernetesClient kubernetesClient) {
    this.reconciler = reconciler;
    this.configuration = configuration;
    this.kubernetesClient = kubernetesClient;

    eventSourceManager = new EventSourceManager<>(this);
    final var context = new EventSourceContext<>(
        eventSourceManager.getControllerResourceEventSource().getResourceCache(),
        configurationService(), kubernetesClient);

    if (reconciler instanceof EventSourceContextInjector) {
      EventSourceContextInjector injector = (EventSourceContextInjector) reconciler;
      injector.injectInto(context);
    }

    prepareEventSources(context).forEach(eventSourceManager::registerEventSource);
  }

  private void waitUntilStartedAndInitContext(R resource, Context context) {
    if (!started.get()) {
      AtomicInteger count = new AtomicInteger(0);
      final var waitTime = 50;
      while (!started.get()) {
        try {
          count.getAndIncrement();
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      log.info("Waited {}ms for controller '{}' to finish initializing", count.get() * waitTime,
          configuration.getName());
    }

    if (reconciler instanceof ContextInitializer) {
      final var initializer = (ContextInitializer<R>) reconciler;
      initializer.initContext(resource, context);
    }
  }

  @Override
  public DeleteControl cleanup(R resource, Context context) {
    waitUntilStartedAndInitContext(resource, context);

    dependents.stream()
        .filter(DependentResourceController::deletable)
        .forEach(dependent -> {
          var dependentResource = dependent.getFor(resource, context);
          if (dependentResource != null) {
            dependent.delete(dependentResource, resource, context);
            logOperationInfo(resource, dependent, dependentResource, "Deleting");
          } else {
            log.info("Ignoring already deleted {} for '{}' {}",
                dependent.getResourceType().getName(),
                resource.getMetadata().getName(),
                configuration.getResourceTypeName());
          }
        });

    return metrics().timeControllerExecution(
        new ControllerExecution<>() {
          @Override
          public String name() {
            return "cleanup";
          }

          @Override
          public String controllerName() {
            return configuration.getName();
          }

          @Override
          public String successTypeName(DeleteControl deleteControl) {
            return deleteControl.isRemoveFinalizer() ? "delete" : "finalizerNotRemoved";
          }

          @Override
          public DeleteControl execute() {
            return reconciler.cleanup(resource, context);
          }
        });
  }

  @Override
  public UpdateControl<R> reconcile(R resource, Context context) {
    waitUntilStartedAndInitContext(resource, context);

    dependents.stream()
        .filter(dependent -> dependent.creatable() || dependent.updatable())
        .forEach(dependent -> {
          var dependentResource = dependent.getFor(resource, context);
          if (dependent.creatable() && dependentResource == null) {
            // we need to create the dependent
            dependentResource = dependent.buildFor(resource, context);
            createOrReplaceDependent(resource, context, dependent, dependentResource, "Creating");
          } else if (dependent.updatable()) {
            dependentResource = dependent.update(dependentResource, resource, context);
            createOrReplaceDependent(resource, context, dependent, dependentResource, "Updating");
          } else {
            logOperationInfo(resource, dependent, dependentResource, "Ignoring");
          }
        });

    return metrics().timeControllerExecution(
        new ControllerExecution<>() {
          @Override
          public String name() {
            return "reconcile";
          }

          @Override
          public String controllerName() {
            return configuration.getName();
          }

          @Override
          public String successTypeName(UpdateControl<R> result) {
            String successType = "resource";
            if (result.isUpdateStatus()) {
              successType = "status";
            }
            if (result.isUpdateResourceAndStatus()) {
              successType = "both";
            }
            return successType;
          }

          @Override
          public UpdateControl<R> execute() {
            return reconciler.reconcile(resource, context);
          }
        });
  }

  // todo: rename variables more explicitly
  private void createOrReplaceDependent(R resource,
      Context context, DependentResourceController dependent,
      Object dependentResource, String operationDescription) {
    // add owner reference if needed
    if (dependentResource instanceof HasMetadata
        && ((KubernetesDependentResourceController) dependent).owned()) {
      ((HasMetadata) dependentResource).addOwnerReference(resource);
    }

    logOperationInfo(resource, dependent, dependentResource, operationDescription);

    // commit the changes
    // todo: add metrics timing for dependent resource
    dependent.createOrReplace(dependentResource, context);
  }

  private void logOperationInfo(R resource, DependentResourceController dependent,
      Object dependentResource, String operationDescription) {
    log.info("{} {} for '{}' {}", operationDescription, dependent.descriptionFor(dependentResource),
        resource.getMetadata().getName(),
        configuration.getResourceTypeName());
  }


  private Metrics metrics() {
    final var metrics = configurationService().getMetrics();
    return metrics != null ? metrics : Metrics.NOOP;
  }

  @Override
  public List<EventSource> prepareEventSources(EventSourceContext<R> context) {
    final List<DependentResource> configured = configuration.getDependentResources();
    dependents = new ArrayList<>(configured.size());

    List<EventSource> sources = new ArrayList<>(configured.size() + 5);
    configured.forEach(dependent -> {
      dependents.add(configuration.dependentFactory().from(dependent));
      sources.add(dependent.initEventSource(context));
    });

    // add manually defined event sources
    if (reconciler instanceof EventSourceInitializer) {
      sources.addAll(((EventSourceInitializer<R>) reconciler).prepareEventSources(context));
    }
    return sources;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Controller<?> that = (Controller<?>) o;
    return configuration.getName().equals(that.configuration.getName());
  }

  @Override
  public int hashCode() {
    return configuration.getName().hashCode();
  }

  @Override
  public String toString() {
    return "'" + configuration.getName() + "' Controller";
  }

  public Reconciler<R> getReconciler() {
    return reconciler;
  }

  public ControllerConfiguration<R> getConfiguration() {
    return configuration;
  }

  public KubernetesClient getClient() {
    return kubernetesClient;
  }

  public MixedOperation<R, KubernetesResourceList<R>, Resource<R>> getCRClient() {
    return kubernetesClient.resources(configuration.getResourceClass());
  }

  /**
   * Registers the specified controller with this operator, overriding its default configuration by
   * the specified one (usually created via
   * {@link io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider#override(ControllerConfiguration)},
   * passing it the controller's original configuration.
   *
   * @throws OperatorException if a problem occurred during the registration process
   */
  public void start() throws OperatorException {
    final Class<R> resClass = configuration.getResourceClass();
    final String controllerName = configuration.getName();
    final var crdName = configuration.getResourceTypeName();
    final var specVersion = "v1";
    log.info("Starting '{}' controller for reconciler: {}, resource: {}", controllerName,
        reconciler.getClass().getCanonicalName(), resClass.getCanonicalName());
    try {
      // check that the custom resource is known by the cluster if configured that way
      final CustomResourceDefinition crd; // todo: check proper CRD spec version based on config
      if (configurationService().checkCRDAndValidateLocalModel()
          && CustomResource.class.isAssignableFrom(resClass)) {
        crd = kubernetesClient.apiextensions().v1().customResourceDefinitions().withName(crdName)
            .get();
        if (crd == null) {
          throwMissingCRDException(crdName, specVersion, controllerName);
        }

        // Apply validations that are not handled by fabric8
        CustomResourceUtils.assertCustomResource(resClass, crd);
      }

      if (failOnMissingCurrentNS()) {
        throw new OperatorException(
            "Controller '"
                + controllerName
                + "' is configured to watch the current namespace but it couldn't be inferred from the current configuration.");
      }
      eventSourceManager.start();
      started.set(true);
      log.info("'{}' controller started, pending event sources initialization", controllerName);
    } catch (MissingCRDException e) {
      throwMissingCRDException(crdName, specVersion, controllerName);
    }
  }

  private ConfigurationService configurationService() {
    if (configurationService == null) {
      configurationService = configuration.getConfigurationService();
      // make sure we always have a default configuration service
      if (configurationService == null) {
        // we shouldn't need to register the configuration with the default service
        configurationService = new BaseConfigurationService(Version.UNKNOWN) {
          @Override
          public boolean checkCRDAndValidateLocalModel() {
            return false;
          }
        };
      }
    }
    return configurationService;
  }

  private void throwMissingCRDException(String crdName, String specVersion, String controllerName) {
    throw new MissingCRDException(
        crdName,
        specVersion,
        "'"
            + crdName
            + "' "
            + specVersion
            + " CRD was not found on the cluster, controller '"
            + controllerName
            + "' cannot be registered");
  }

  /**
   * Determines whether we should fail because the current namespace is request as target namespace
   * but is missing
   *
   * @return {@code true} if the current namespace is requested but is missing, {@code false}
   *         otherwise
   */
  private boolean failOnMissingCurrentNS() {
    if (configuration.watchCurrentNamespace()) {
      final var effectiveNamespaces = configuration.getEffectiveNamespaces();
      return effectiveNamespaces.size() == 1
          && effectiveNamespaces.stream().allMatch(Objects::isNull);
    }
    return false;
  }

  public EventSourceManager<R> getEventSourceManager() {
    return eventSourceManager;
  }

  public void stop() {
    if (eventSourceManager != null) {
      eventSourceManager.stop();
    }
    started.set(false);
  }
}
