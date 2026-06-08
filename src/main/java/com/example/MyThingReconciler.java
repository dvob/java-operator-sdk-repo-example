package com.example;

import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ControllerConfiguration(generationAwareEventProcessing = false)
public class MyThingReconciler implements Reconciler<MyThing> {

    private static final Logger log = LoggerFactory.getLogger(MyThingReconciler.class);

    @Override
    public UpdateControl<MyThing> reconcile(MyThing resource, Context<MyThing> context) {
        log.info("Reconciling MyThing: {}", resource.getMetadata().getName());

        List<String> configs = context.getSecondaryResources(MyThingConfig.class).stream()
                .map(config -> config.getMetadata().getName())
                .sorted()
                .collect(Collectors.toList());

        log.info("MyThing {} has configs: {}", resource.getMetadata().getName(), configs);

        if (resource.getStatus() == null) {
            resource.setStatus(new MyThingStatus());
        }
        resource.getStatus().setConfigs(configs);

        return UpdateControl.patchStatus(resource);
    }

    @Override
    public List<EventSource<?, MyThing>> prepareEventSources(EventSourceContext<MyThing> context) {
        // Declare first so the primaryToSecondaryMapper lambda can reference it
        final InformerEventSource<MyThingConfig, MyThing>[] holder = new InformerEventSource[1];

        var config = InformerEventSourceConfiguration
                .from(MyThingConfig.class, MyThing.class)
                .withName("myThingConfigs")
                .withPrimaryToSecondaryMapper(primary ->
                        holder[0].list(c ->
                                        primary.getMetadata().getName().equals(c.getSpec().getMyThingRef())
                                        && primary.getMetadata().getNamespace().equals(c.getMetadata().getNamespace()))
                                .map(ResourceID::fromResource)
                                .collect(Collectors.toSet())
                )
                .withSecondaryToPrimaryMapper(new SecondaryToPrimaryMapper<MyThingConfig>() {
                    @Override
                    public Set<ResourceID> toPrimaryResourceIDs(MyThingConfig resource) {
                        if (resource.getSpec().getMyThingRef() == null) {
                            return Collections.emptySet();
                        }
                        var target = new ResourceID(resource.getSpec().getMyThingRef(), resource.getMetadata().getNamespace());
                        log.info("secondaryToPrimary: {} -> {}", resource.getMetadata().getName(), target);
                        return Set.of(target);
                    }

                    @Override
                    public Set<ResourceID> toPrimaryResourceIDs(MyThingConfig newResource, MyThingConfig oldResource) {
                        var targets = new HashSet<>(toPrimaryResourceIDs(newResource));
                        if (oldResource != null && oldResource.getSpec().getMyThingRef() != null) {
                            targets.add(new ResourceID(oldResource.getSpec().getMyThingRef(), oldResource.getMetadata().getNamespace()));
                        }
                        return targets;
                    }
                })
                .build();

        holder[0] = new InformerEventSource<>(config, context);
        return List.of(holder[0]);
    }
}
