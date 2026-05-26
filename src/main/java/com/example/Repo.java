package com.example;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("com.example")
@Version("v1")
public class Repo extends CustomResource<RepoSpec, RepoStatus> implements Namespaced {
}
