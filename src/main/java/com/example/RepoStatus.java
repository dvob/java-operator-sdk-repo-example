package com.example;

import java.util.List;

public class RepoStatus {

    // Repos that have joined this repo (populated by reconciler for group repos)
    private List<String> repos;

    public List<String> getRepos() {
        return repos;
    }

    public void setRepos(List<String> repos) {
        this.repos = repos;
    }
}
