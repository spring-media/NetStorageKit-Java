package com.akamai.netstorage.service;

import com.akamai.netstorage.DefaultCredential;
import com.akamai.netstorage.NetStorage;
import com.akamai.netstorage.exception.*;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.IllegalArgumentException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class uses the akamai java netstorage API and adds functionality
 */
public class NetstorageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetstorageService.class);

    final MappingJackson2XmlHttpMessageConverter xmlConverter;
    final ThreadPoolTaskExecutor listingExecutor;
    final ThreadPoolTaskExecutor deletionExecutor;

    final String netstorageHost;
    final String netstorageUser;
    final String netstorageKey;
    final String netstorageFolder;

    final NetStorage netStorage;

    public NetstorageService(String netstorageHost,
                             String netstorageUser,
                             String netstorageKey,
                             String netstorageFolder, // aka CP-Code example: "/12345"
                             ThreadPoolTaskExecutor listingExecutor,
                             ThreadPoolTaskExecutor deletionExecutor,
                             MappingJackson2XmlHttpMessageConverter xmlConverter) {
        this.netstorageHost = netstorageHost;
        this.netstorageUser = netstorageUser;
        this.netstorageKey = netstorageKey;
        this.netstorageFolder = netstorageFolder;
        this.netStorage = new NetStorage(new DefaultCredential(netstorageHost, netstorageUser, netstorageKey));
        this.listingExecutor = listingExecutor;
        this.deletionExecutor = deletionExecutor;
        this.xmlConverter = xmlConverter;
    }

    /**
     * Resolves all entries of this given path without recursively fetching sub directories
     * If server responds with a resume entry, the returned iterable takes care of it and fires subsequent requests
     *
     * @param path path of a directory
     * @return an iterator of nodes contained in the requested directory (the given directory is not part of the response)
     */
    public Iterator<Node> dir(String path) {
        return new Iterator<Node>() {

            NetstorageXML.Resume resume = null;
            Iterator<Node> nextBatch = getNextBatch();

            Iterator<Node> getNextBatch() {
                try {
                    Map<String, String> additionalParams = new HashMap<>();
//                    if (maxEntries != null) {
//                        additionalParams.put("max_entries", Integer.valueOf(maxEntries).toString());
//                    }

                    if (nextBatch != null) {
                        if (resume == null) {
                            return Collections.emptyIterator();
                        }
                        additionalParams.put("start", resume.getStart());
                    }

                    NetstorageXML.Stat stat = convertXMLToStat(netStorage.dir(netstorageFolder + path, "xml", additionalParams));
                    resume = stat.resume;
                    List<Node> nodes = fromStat(stat);
                    return nodes.iterator();
                } catch (NetStorageException e) {
                    LOGGER.warn("Some exception occurred while fetching path {}", path, e);
                    throw e;
                }
            }

            @Override
            public boolean hasNext() {
                return nextBatch.hasNext();
            }

            @Override
            public Node next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("End of file listing reached for path " + path);
                }
                Node next = nextBatch.next();
                if (!nextBatch.hasNext()) {
                    nextBatch = getNextBatch();
                }
                return next;
            }
        };
    }

    /**
     * Resolves all files (directories, files, symlinks), implicit as well as explicit.
     * The returned node is a tree of the whole file structure within the current CP code.
     *
     * @return the root node for the current CP code
     * @throws com.akamai.netstorage.exception.FileNotFoundException if the directory specified by directoryName does not exist
     */
    public ListingDirectory list(String directoryName) {
        String directory = (directoryName.endsWith("/") ? directoryName.substring(0, directoryName.length() - 1) : directoryName);
        Iterator<Node> nodesIterator = new Iterator<Node>() {
            NetstorageXML.Resume resume = null;
            Iterator<Node> nextBatch = getNextBatch();

            Iterator<Node> getNextBatch() {
                try {
                    Map<String, String> additionalParams = new HashMap<>();
                    if (nextBatch != null) {
                        if (resume == null) {
                            return Collections.emptyIterator();
                        }
                        additionalParams.put("start", resume.getStart());
                    }
                    // only if its not the root directory, we need to provide "end" to limit results to the given directory
                    // (@see https://learn.akamai.com/en-us/webhelp/netstorage/netstorage-http-api-developer-guide/GUID-B02EF535-4A35-47B8-A81C-1088B6BCDEFA.html)
                    if (directory.length() > 0) {
                        additionalParams.put("end", netstorageFolder + directory + "0");
                    }

                    InputStream list = netStorage.list(netstorageFolder + directory, additionalParams);

                    NetstorageXML.NetstorageList netstorageList = xmlConverter.getObjectMapper().readerFor(NetstorageXML.NetstorageList.class).readValue(list);
                    resume = netstorageList.resume;
                    List<Node> nodes = fromNetstorageList(netstorageList);

                    return nodes.iterator();
                } catch (NetStorageException e) {
                    LOGGER.warn("Some exception occurred while fetching directory {}", directory, e);
                    throw e;
                } catch (IOException e) {
                    LOGGER.error("Some IOException while reading input from netstorage.");
                    throw new ConnectionException("Some IOException while reading input from netstorage.", e);
                }
            }

            private List<Node> fromNetstorageList(NetstorageXML.NetstorageList netstorageList) {
                List<Node> nodes = new ArrayList<>();
                for (NetstorageXML.File file : netstorageList.files) {
                    String path = file.name.substring(netstorageFolder.length() - 1);
                    String directory = path.substring(0, path.lastIndexOf('/') + 1);
                    String fileName = path.substring(directory.length());

                    switch (file.getType()) {
                        case "dir": {
                            nodes.add(new ListingDirectory(directory, fileName, false));
                        }
                        break;
                        case "file": {
                            nodes.add(new File(directory, fileName, file.getSize(), file.getMd5(), file.getMtime()));
                        }
                        break;
                        case "symlink": {
                            nodes.add(new Symlink(directory, fileName, null, 0));
                        }
                        break;
                        default: {
                            LOGGER.warn("Unrecognised netstorage type {}", file.getType());
                        }
                    }
                }
                return nodes;
            }

            @Override
            public boolean hasNext() {
                return nextBatch.hasNext();
            }

            @Override
            public Node next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("End of file listing reached for directory " + directory);
                }
                Node next = nextBatch.next();
                if (!nextBatch.hasNext()) {
                    nextBatch = getNextBatch();
                }
                return next;
            }
        };

        // build file structure as node tree
        ListingDirectory root = new ListingDirectory(directory, false);
        Map<String, Node> globalMapping = new HashMap<>();
        while (nodesIterator.hasNext()) {
            Node node = nodesIterator.next();
            root.insert(node, globalMapping);
        }

        return root;
    }

    List<Node> fromStat(NetstorageXML.Stat stat) {
        if (stat == null) {
            return Collections.EMPTY_LIST;
        }
        List<Node> nodes = new ArrayList<>();
        for (NetstorageXML.File file : stat.getFiles()) {
            String directory;
            String fileName = file.getName();
            if (stat.getDirectory().length() == 0) {
                directory = "";
                fileName = "";
            } else {
                directory = (stat.getDirectory() + "/").substring(netstorageFolder.length());
            }
            if (fileName.endsWith("/")) {
                fileName = fileName.substring(0, fileName.length() - 1);
            }

            switch (file.getType()) {
                case "dir": {
                    nodes.add(new Directory(directory, fileName, file.isImplicit(), file.getBytes(), file.getFiles(), file.getMtime()));
                }
                break;
                case "file": {
                    nodes.add(new File(directory, fileName, file.getSize(), file.getMd5(), file.getMtime()));
                }
                break;
                case "symlink": {
                    nodes.add(new Symlink(directory, fileName, file.getTarget(), file.getMtime()));
                }
                break;
                default: {
                    LOGGER.warn("Unrecognised netstorage type {}", file.getType());
                }
            }
        }
        return nodes;
    }

    /**
     * @param path netstorage path
     * @return Stat or Exception thrown
     */
    public Node stat(String path) {
        try {
            return fromStat(convertXMLToStat(netStorage.statIncludingImplicit(netstorageFolder + path))).get(0);
        } catch (NetStorageException e) {
            LOGGER.info("Stat for path {} could not be retreived (any longer?). Maybe it was deleted meanwhile.");
            throw e;
        }
    }


    /**
     * 404s are being thrown as {@link FileNotFoundException} -> only if the node specified with startPath is absent
     * Resolves the whole file structure recursively
     * Attention: This is similyr to {@link NetstorageService#list(String)} but quite inefficient as each directory triggers
     * a new requests (in contrast to just a single request when using list(String))
     * On the other hand, list(String) does not provide all information (symlinks are incomplete as they reveal no target e.g.)
     *
     * @param startPath starting path for file structure
     * @param <N>       expected return type
     * @return The starting node startPath is pointing to (all children are resolved as well)
     */
    public <N extends Node> N dirComplete(String startPath) {
        try {
            NetstorageXML.Stat stat = convertXMLToStat(netStorage.statIncludingImplicit(netstorageFolder + startPath));
            List<Node> nodes = fromStat(stat);
            if (nodes.size() != 1) {
                throw new IllegalArgumentException("Exactly one result expected for (" + startPath + ").");
            }
            Node node = nodes.get(0);
            node.resolveChildren().get();
            return (N) node;

        } catch (NetStorageException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Calculating file structure was interrupted.", e);
        } catch (ExecutionException e) {
            throw new UnspecificNetstorageException("Dir() threw ExecutionException.", e);
        }
    }


    /**
     * Deletes the (empty!) directory or the file specified by path
     * If you need to delete recursively use {@link NetstorageService#quickDelete(Node)}
     *
     * @param path file or directory to delete
     * @throws FileNotFoundException or other exceptions
     */
    public void delete(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Deleting the whole netstorage bucket is disallowed currently! Please specify at least a first level folder to be deleted.");
        }
        String netstoragePath = netstorageFolder + path;
        try {
            if (!netStorage.delete(netstoragePath)) {
                throw new UnspecificNetstorageException("Error while deleting path " + path + " Netstorage returned false");
            }
        } catch (NetStorageException e) {
            LOGGER.error("Could not delete {}", netstoragePath, e);
            throw e;
        }
    }

    private NetstorageXML.Stat convertXMLToStat(InputStream inputStream) {
        List<NetstorageXML.File> fileList = new ArrayList<>();
        try {
            NetstorageXML.Stat stat = xmlConverter.getObjectMapper().readerFor(NetstorageXML.Stat.class).readValue(inputStream);
            LOGGER.debug("Found {} entries for path {}", stat.files.size(), stat.getDirectory());
            for (NetstorageXML.File file : stat.files) {
                String subFilePath = stat.getDirectory() + "/" + file.getName();
                LOGGER.debug("Found {} of type {}", subFilePath, file.getType());
                fileList.add(file);
            }
            LOGGER.info("Returning {} files for directory {}", fileList.size(), stat.getDirectory());
            return stat;
        } catch (IOException e) {
            throw new DeserializationException("Deserialization problem.", e);
        }
    }

    /**
     * Deletes the content (recursively) within the given directory and deletes the directory itself in the end
     * 404s are being ignored and do not throw an exception
     *
     * @param node a node to delete recursively, 404s are being ignored and do not (re)throw an exception
     */
    public void quickDelete(Node node) {
        try {
            node.deleteRecursively().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Quick delete interrupted.", e);
            throw new UnspecificNetstorageException("Quick delete interrupted.", e);
        } catch (ExecutionException e) {
            throw new UnspecificNetstorageException("Quick delete threw ExecutionException.", e);
        }
    }

    /**
     * Deletes the content (recursively) within the given directory and deletes the directory itself in the end
     *
     * @param path a directory (404s will be ignored)
     */
    public void quickDelete(String path) {
        try {
            Node dir = dirComplete(path);
            quickDelete(dir);
        } catch (FileNotFoundException e) {
            LOGGER.info("Path {} not found in netstorage. Ignoring.", path);
        }
    }

    /**
     * If activated within your nestorage configuration, use this method to delete a folder recursively.
     * Keep in mind, that this function returns immediately and just triggers a recursive deletion within netstrage.
     * The process of deletion may take a while.
     * If you need to block until content is fully removed, use {@link NetstorageService#quickDelete(Node)} instead
     *
     * @param path Directory to delete
     * @return true on success
     */
    public boolean realQuickDelete(String path) {
        try {
            return netStorage.quickDelete(netstorageFolder + path);
        } catch (NetStorageException e) {
            LOGGER.error("Could not quick delete {}", path, e);
            throw e;
        }
    }

    public void upload(String path, InputStream inputStream) {
        netStorage.upload(netstorageFolder + path, inputStream);
    }

    public void symlink(String path, String target) {
        netStorage.symlink(netstorageFolder + path, netstorageFolder + target);
    }

    /**
     * POJOs for netstorage xml responses
     */
    static class NetstorageXML {
        @JacksonXmlRootElement(localName = "list")
        @ToString
        static class NetstorageList {
            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "file")
            List<File> files = new ArrayList<>();
            Resume resume;
        }

        @Getter
        @ToString
        static class Stat {
            String directory;
            @JacksonXmlElementWrapper(useWrapping = false)
            @JacksonXmlProperty(localName = "file")
            List<File> files = new ArrayList<>();
            Resume resume;
        }

        @Getter
        @ToString
        static class File {
            String type;
            String target;
            String name;
            long bytes;
            long files;
            long mtime;
            boolean implicit;
            long size;
            String md5;
        }

        @Getter
        @ToString
        static class Resume {
            String start;
        }
    }

    @Getter
    public class Symlink extends Node {
        final String targetPath;
        final long mtime;

        public Symlink(String directory, String file, String targetPath, long mtime) {
            super(directory, file);
            this.targetPath = targetPath;
            this.mtime = mtime;
        }

        @Override
        CompletableFuture<Void> deleteRecursively() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    LOGGER.info("Deleting {}", this);
                    delete(getPath());
                } catch (FileNotFoundException e) {
                    LOGGER.info("404 while deleting symlink {} {}", this, e.getMessage());
                }
                return null;
            }, deletionExecutor);
        }

        @Override
        CompletableFuture<Void> resolveChildren() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String toString() {
            return "Symlink{" +
                    "targetPath='" + targetPath + '\'' +
                    ", mtime=" + mtime +
                    ", directory='" + directory + '\'' +
                    ", file='" + file + '\'' +
                    '}';
        }
    }

    @Getter
    public class File extends Node {
        final long size;
        final String md5;
        final long mtime;

        public File(String directory, String file, long size, String md5, long mtime) {
            super(directory, file);
            this.size = size;
            this.md5 = md5;
            this.mtime = mtime;
        }


        @Override
        public int countFiles() {
            return 1;
        }

        @Override
        CompletableFuture<Void> deleteRecursively() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    LOGGER.info("Deleting {}", this);
                    delete(getPath());
                } catch (FileNotFoundException e) {
                    LOGGER.info("404 while deleting file {} {}", this, e.getMessage());
                }
                return null;
            }, deletionExecutor);
        }

        @Override
        CompletableFuture<Void> resolveChildren() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String toString() {
            return "File{" +
                    "size=" + size +
                    ", md5='" + md5 + '\'' +
                    ", mtime=" + mtime +
                    ", directory='" + directory + '\'' +
                    ", file='" + file + '\'' +
                    '}';
        }
    }

    static class DirectoryFile {
        final String directory;
        final String file;

        public DirectoryFile(String directory, String file) {
            this.directory = directory;
            this.file = file;
        }

        static DirectoryFile fromPath(String path) {
            if (path == null || path.length() == 0) {
                return new DirectoryFile("", "");
            }
            int endIndex = path.lastIndexOf("/");
            String directory = path.substring(0, endIndex) + "/";
            String file = path.substring(endIndex + 1);
            return new DirectoryFile(directory, file);
        }
    }

    @Getter
    public class ListingDirectory extends Node {
        final boolean implicit;
        final List<Node> children = new ArrayList<>();
        final String pathIncludingTrailingSlash;

        public ListingDirectory(String path, boolean implicit) {
            this(DirectoryFile.fromPath(path), implicit);
        }

        ListingDirectory(DirectoryFile directoryFile, boolean implicit) {
            this(directoryFile.directory, directoryFile.file, implicit);
        }

        public ListingDirectory(String directory, String file, boolean implicit) {
            super(directory, file);
            this.implicit = implicit;
            this.pathIncludingTrailingSlash = path + "/";
        }

        @Override
        public String getPath() {
            return pathIncludingTrailingSlash;
        }

        @Override
        CompletableFuture<Void> deleteRecursively() {
            LOGGER.info("start deleting {}", this);
            CompletableFuture<Void>[] completableFutures = new CompletableFuture[children.size()];
            int count = 0;
            for (Node child : children) {
                completableFutures[count++] = child.deleteRecursively();
            }
            return resolveChildren().thenCompose(voidResult -> CompletableFuture.allOf(completableFutures)
                    .thenApplyAsync(aVoid -> {
                        if (!isImplicit()) {
                            try {
                                LOGGER.info("Deleting {}", this);
                                delete(getPath());
                            } catch (FileNotFoundException e) {
                                LOGGER.info("404 while deleting directory {} {}", this, e.getMessage());
                            }
                        }
                        return null;
                    }, deletionExecutor));
        }

        /**
         * All children are per definition resolved in case a listing was requested
         *
         * @return null
         */
        @Override
        CompletableFuture<Void> resolveChildren() {
            return CompletableFuture.completedFuture(null);
        }


        boolean addChild(Node child) {
            return children.add(child);
        }


        @Override
        public int nodeCount() {
            int childCount = 1;
            for (Node child : children) {
                childCount += child.nodeCount();
            }
            return childCount;
        }

        @Override
        public int countFiles() {
            int fileCount = 0;
            for (Node child : children) {
                fileCount += child.countFiles();
            }
            return fileCount;
        }

        /**
         * inserts the given node into the structure at the correct position
         *
         * @param node the node to be inserted
         * @return the node the given node was inserted to
         */
        Node insert(Node node, Map<String, Node> globalMapping) {
            if (!node.getDirectory().startsWith(getDirectory())) {
                return null;
            }

            if (node.getDirectory().equals(getPath())) {
                addChild(node);
                return this;
            } else {
                Node childFound = globalMapping.get(node.getDirectory());
                if (childFound instanceof ListingDirectory) {
                    Node insert = ((ListingDirectory) childFound).insert(node, globalMapping);
                    if (insert != null) {
                        return insert;
                    }
                }
            }
            // we need to create implicit directories...

            String directoryWithoutTrailingSlash = node.getDirectory().substring(0, node.getDirectory().length() - 1);
            int beginIndex = directoryWithoutTrailingSlash.lastIndexOf("/") + 1;
            String upperLevelDirectoryName = directoryWithoutTrailingSlash.substring(beginIndex);
            String upperLevelDirectoryPath = node.getDirectory().substring(0, beginIndex);

            ListingDirectory implicitDirectory = new ListingDirectory(upperLevelDirectoryPath, upperLevelDirectoryName, true);
            globalMapping.put(implicitDirectory.getPath(), implicitDirectory);
            LOGGER.info("Creating directory {}", implicitDirectory);
            implicitDirectory.insert(node, globalMapping);
            return insert(implicitDirectory, globalMapping);
        }

        public Collection<Node> getChildren() {
            return children;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("ListingDirectory{");
            sb.append("implicit=").append(implicit);
            sb.append(", children=").append(children);
            sb.append(", pathIncludingTrailingSlash='").append(pathIncludingTrailingSlash).append('\'');
            sb.append(", path='").append(path).append('\'');
            sb.append(", directory='").append(directory).append('\'');
            sb.append(", file='").append(file).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    @Getter
    public class Directory extends ListingDirectory {
        transient boolean childrenResolved = false;
        final long bytes;
        final long files;
        final long mtime;


        public Directory(String directory, String file, boolean implicit, long bytes, long files, long mtime) {
            super(directory, file, implicit);
            this.bytes = bytes;
            this.files = files;
            this.mtime = mtime;
        }

        @Override
        synchronized CompletableFuture<Void> resolveChildren() {
            if (childrenResolved) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture
                    .supplyAsync(() -> {
                        Iterator<Node> nodes = dir(getPath());
                        return nodes;
                    }, listingExecutor)
                    .exceptionally(throwable -> {
                        if ((throwable instanceof FileNotFoundException)) {
                            return Collections.emptyIterator();
                        } else if (throwable instanceof NetStorageException) {
                            throw (NetStorageException) throwable;
                        } else {
                            throw new UnspecificNetstorageException("Exception while resolving children of " + this, throwable);
                        }
                    })
                    .thenCompose(nodes ->
                    {
                        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
                        while (nodes.hasNext()) {
                            Node node = nodes.next();
                            addChild(node);
                            completableFutures.add(node.resolveChildren());
                        }
                        CompletableFuture<Void> result = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
                        childrenResolved = true;
                        return result;
                    });
        }

        @Override
        public String toString() {
            return "Directory{" +
                    "implicit=" + implicit +
                    ", bytes=" + bytes +
                    ", files=" + files +
                    ", mtime=" + mtime +
                    ", children=" + children +
                    ", directory='" + directory + '\'' +
                    ", file='" + file + '\'' +
                    '}';
        }


    }
}
