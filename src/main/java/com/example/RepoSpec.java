package com.example;

import java.util.List;

public class RepoSpec {

    // List of .metadata.name of Repo resources this repo wants to join
    private List<String> joinRepos;

    public List<String> getJoinRepos() {
        return joinRepos;
    }

    public void setJoinRepos(List<String> joinRepos) {
        this.joinRepos = joinRepos;
    }
}
