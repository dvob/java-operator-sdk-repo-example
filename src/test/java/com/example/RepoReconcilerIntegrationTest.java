package com.example;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

class RepoReconcilerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(RepoReconcilerIntegrationTest.class);

    @RegisterExtension
    LocallyRunOperatorExtension extension =
            LocallyRunOperatorExtension.builder()
                    .withReconciler(RepoReconciler.class)
                    .build();

    @Test
    void joinersShouldAppearInGroupStatus() throws InterruptedException {
        log.info("--- Creating mygroup and repo-a (joins mygroup) ---");
        extension.create(repo("mygroup", null));
        extension.create(repo("repo-a", List.of("mygroup")));

        log.info("--- Waiting for mygroup.status.repos to contain repo-a ---");
        extension.getKubernetesClient()
                .resources(Repo.class)
                .inNamespace(extension.getNamespace())
                .withName("mygroup")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getRepos() != null
                                && r.getStatus().getRepos().contains("repo-a"),
                        30, TimeUnit.SECONDS);
        log.info("--- mygroup.status.repos contains repo-a ---");

        log.info("--- Removing mygroup from repo-a's joinRepos ---");
        var repoA = extension.get(Repo.class, "repo-a");
        repoA.getSpec().setJoinRepos(List.of());
        extension.replace(repoA);

//        log.info("--- Update mygroup with annotation to trigger reconciliation ---");
//        var mygroup = extension.get(Repo.class, "mygroup");
//        mygroup.getMetadata().setAnnotations(java.util.Map.of("trigger", "now"));
//        extension.replace(mygroup);

        log.info("--- Waiting (5s) for mygroup.status.repos to become empty ---");
        extension.getKubernetesClient()
                .resources(Repo.class)
                .inNamespace(extension.getNamespace())
                .withName("mygroup")
                .waitUntilCondition(
                        r -> r != null
                                && r.getStatus() != null
                                && r.getStatus().getRepos() != null
                                && r.getStatus().getRepos().isEmpty(),
                        5, TimeUnit.SECONDS);
        log.info("--- mygroup.status.repos is empty ---");
    }

    private Repo repo(String name, List<String> joinRepos) {
        var resource = new Repo();
        resource.setMetadata(new ObjectMetaBuilder().withName(name).build());
        resource.setSpec(new RepoSpec());
        resource.getSpec().setJoinRepos(joinRepos);
        return resource;
    }
}
