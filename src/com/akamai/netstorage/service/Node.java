package com.akamai.netstorage.service;

import lombok.Getter;

import java.util.concurrent.CompletableFuture;

@Getter
public abstract class Node {
    final String path;
    final String directory; // starts and ends with a "/"
    final String file;

    public Node(String directory, String file) {
        this.directory = directory;
        this.file = file;
        this.path = directory + file;
    }

    public String getPath() {
        return path;
    }

    /**
     * @return amount odf all nodes, including directories, files, symlinks starting at this node inclusive
     */
    public int nodeCount()
    {
        return 1;
    }

    /**
     * @return counts all files (no smylinks and no directories)
     */
    public int countFiles() {
        return 0;
    }
    /**
     * deletes the contents of this node including the node itself, ignoring 404s
     * @throws com.akamai.netstorage.exception.NetStorageException in case of other errors (404s are being ignored and do not (re)throw an exception)
     */
    abstract CompletableFuture<Void> deleteRecursively();

    /**
     * resolves all children recursively, 404s are being ignored as these can be the result of concurrent
     * deletion of content by other parties
     * @throws com.akamai.netstorage.exception.NetStorageException in case not 404 errors occured but other error scenarios (500, 409, etc.)
     */
    abstract CompletableFuture<Void> resolveChildren();

    @Override
    public String toString() {
        return "Node{" +
                "directory='" + directory + '\'' +
                ", file='" + file + '\'' +
                '}';
    }
}
