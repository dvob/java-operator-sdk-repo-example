package com.example;

import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
        // Watch MyThingConfig as secondary resources of MyThing.
        // No withPrimaryToSecondaryMapper needed — JOSDK builds the reverse index
        // automatically from the secondaryToPrimaryMapper.
        var configEventSource = new InformerEventSource<>(
                InformerEventSourceConfiguration
                        .from(MyThingConfig.class, MyThing.class)
                        .withName("myThingConfigs")
                        .withSecondaryToPrimaryMapper(secondary -> {
                            if (secondary.getSpec().getMyThingRef() == null) {
                                return Collections.emptySet();
                            }
                            var target = new ResourceID(secondary.getSpec().getMyThingRef(), secondary.getMetadata().getNamespace());
                            log.info("secondaryToPrimary: {} -> {}", secondary.getMetadata().getName(), target);
                            return Set.of(target);
                        })
                        .build(),
                context);

        return List.of(configEventSource);
    }
}
