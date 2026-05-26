package com.example;

import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ControllerConfiguration(generationAwareEventProcessing = false)
public class RepoReconciler implements Reconciler<Repo> {

    private static final Logger log = LoggerFactory.getLogger(RepoReconciler.class);

    @Override
    public UpdateControl<Repo> reconcile(Repo resource, Context<Repo> context) {
        log.info("Reconciling: {}", resource.getMetadata().getName());

        // Collect repos that want to join this one
        List<String> joiners = context.getSecondaryResources(Repo.class).stream()
                .filter(joiner -> joiner.getSpec().getJoinRepos() != null
                        && joiner.getSpec().getJoinRepos().contains(resource.getMetadata().getName()))
                .map(joiner -> joiner.getMetadata().getName())
                .sorted()
                .collect(Collectors.toList());

        log.info("Repo {} has joiners: {}", resource.getMetadata().getName(), joiners);

        if (resource.getStatus() == null) {
            resource.setStatus(new RepoStatus());
        }
        resource.getStatus().setRepos(joiners);

        return UpdateControl.patchStatus(resource);
    }

    @Override
    public List<EventSource<?, Repo>> prepareEventSources(EventSourceContext<Repo> context) {
        // Same-type event source: Repo watches Repo
        // Wired exactly like ArtifactoryRepositoryReconciler in our real operator
        var config = InformerEventSourceConfiguration
                        .from(Repo.class, Repo.class)
                        .withName("repoJoiners")
                        .withPrimaryToSecondaryMapper(primary -> {
                            return context.getPrimaryCache().list()
                                    .filter(repo -> repo.getMetadata().getNamespace().equals(primary.getMetadata().getNamespace()))
                                    .filter(repo -> repo.getSpec().getJoinRepos() != null
                                            && repo.getSpec().getJoinRepos().contains(primary.getMetadata().getName()))
                                    .map(ResourceID::fromResource)
                                    .collect(Collectors.toSet());
                        })
                        .withSecondaryToPrimaryMapper(secondary -> {
                            if (secondary.getSpec().getJoinRepos() == null) {
                                return Collections.emptySet();
                            }
                            Set<ResourceID> targets = secondary.getSpec().getJoinRepos().stream()
                                    .map(name -> new ResourceID(name, secondary.getMetadata().getNamespace()))
                                    .collect(Collectors.toSet());
                            log.info("secondaryToPrimary: {} -> {}", secondary.getMetadata().getName(), targets);
                            return targets;
                        })
                        .build();

        // Override onUpdate to run the secondaryToPrimaryMapper on BOTH old and new object,
        // like controller-runtime does (see enqueue_mapped.go). The default only maps the new
        // object, so removals from joinRepos would not notify the former target.
        var repoEventSource = new InformerEventSource<>(config, context) {
            @Override
            public void onUpdate(Repo oldObject, Repo newObject) {
                // Propagate for oldObject first so removed refs are notified,
                // then let super handle newObject (the default behavior).
                var handler = getEventHandler();
                if (handler != null) {
                    for (var id : config.getSecondaryToPrimaryMapper().toPrimaryResourceIDs(oldObject)) {
                        handler.handleEvent(new Event(id));
                    }
                }
                super.onUpdate(oldObject, newObject);
            }
        };

        return List.of(repoEventSource);
    }
}
