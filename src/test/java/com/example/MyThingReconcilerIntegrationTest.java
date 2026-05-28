package com.example;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

class MyThingReconcilerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(MyThingReconcilerIntegrationTest.class);

    @RegisterExtension
    LocallyRunOperatorExtension extension =
            LocallyRunOperatorExtension.builder()
                    .withReconciler(MyThingReconciler.class)
                    .build();

    @Test
    void configsShouldAppearInMyThingStatus() {
        log.info("--- Creating my-thing and config-a (references my-thing) ---");
        extension.create(myThing("my-thing"));
        extension.create(myThingConfig("config-a", "my-thing"));

        log.info("--- Waiting for my-thing.status.configs to contain config-a ---");
        extension.getKubernetesClient()
                .resources(MyThing.class)
                .inNamespace(extension.getNamespace())
                .withName("my-thing")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getConfigs() != null
                                && r.getStatus().getConfigs().contains("config-a"),
                        30, TimeUnit.SECONDS);
        log.info("--- my-thing.status.configs contains config-a ---");

        log.info("--- Creating config-b (also references my-thing) ---");
        extension.create(myThingConfig("config-b", "my-thing"));

        log.info("--- Waiting for my-thing.status.configs to contain both ---");
        extension.getKubernetesClient()
                .resources(MyThing.class)
                .inNamespace(extension.getNamespace())
                .withName("my-thing")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getConfigs() != null
                                && r.getStatus().getConfigs().containsAll(java.util.List.of("config-a", "config-b")),
                        30, TimeUnit.SECONDS);
        log.info("--- my-thing.status.configs contains config-a and config-b ---");

        log.info("--- Deleting config-a ---");
        extension.delete(extension.get(MyThingConfig.class, "config-a"));

        log.info("--- Waiting for my-thing.status.configs to only contain config-b ---");
        extension.getKubernetesClient()
                .resources(MyThing.class)
                .inNamespace(extension.getNamespace())
                .withName("my-thing")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getConfigs() != null
                                && r.getStatus().getConfigs().equals(java.util.List.of("config-b")),
                        30, TimeUnit.SECONDS);
        log.info("--- my-thing.status.configs contains only config-b ---");

        log.info("--- Changing config-b to reference a different MyThing ---");
        var configB = extension.get(MyThingConfig.class, "config-b");
        configB.getSpec().setMyThingRef("some-other-thing");
        extension.replace(configB);

        log.info("--- Waiting (5s) for my-thing.status.configs to become empty ---");
        extension.getKubernetesClient()
                .resources(MyThing.class)
                .inNamespace(extension.getNamespace())
                .withName("my-thing")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getConfigs() != null
                                && r.getStatus().getConfigs().isEmpty(),
                        5, TimeUnit.SECONDS);
        log.info("--- my-thing.status.configs is empty ---");
    }

    private MyThing myThing(String name) {
        var resource = new MyThing();
        resource.setMetadata(new ObjectMetaBuilder().withName(name).build());
        resource.setSpec(new MyThingSpec());
        return resource;
    }

    private MyThingConfig myThingConfig(String name, String myThingRef) {
        var resource = new MyThingConfig();
        resource.setMetadata(new ObjectMetaBuilder().withName(name).build());
        resource.setSpec(new MyThingConfigSpec());
        resource.getSpec().setMyThingRef(myThingRef);
        return resource;
    }
}
