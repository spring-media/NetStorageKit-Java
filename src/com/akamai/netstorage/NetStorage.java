/*
 * Copyright 2014 Akamai Technologies http://developer.akamai.com.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.akamai.netstorage;

import com.akamai.auth.RequestSigner;
import com.akamai.auth.RequestSigningException;
import com.akamai.builders.*;
import com.akamai.netstorage.NetStorageCMSv35Signer.NetStorageType;
import com.akamai.netstorage.exception.IllegalArgumentException;
import com.akamai.netstorage.exception.*;

import java.io.FileNotFoundException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.akamai.netstorage.Utils.readToEnd;

/**
 * The Netstorage class is the preferred interface for calling libraries indending to leverage the Netstorage API.
 * All of the available actions are innumerated in this library and are responsible for the correct business
 * logic to assemble the request to the API. Some early safetys are added in this library to limit errors.
 * <p>
 * TODO: Add "LIST" support for ObjectStore
 * TODO: Detect FileStore v. ObjectStore
 * TODO: Extract xml response from various requests into standard object representation
 *
 * @author colinb@akamai.com (Colin Bendell)
 */
public class NetStorage {

    private DefaultCredential credential;

    // defaults
    private int connectTimeout = 15000;
    private int readTimeout = 15000;

    public NetStorage(DefaultCredential credential) {
        this.credential = credential;
    }

    public NetStorage(DefaultCredential credential, int connectTimeout, int readTimeout) {
        this.credential = credential;
        if (connectTimeout > 0) this.setConnectTimeout(connectTimeout);
        if (readTimeout > 0) this.setReadTimeout(readTimeout);
    }

    protected URL getNetstorageUri(String path) {
        try {
            if (!path.startsWith("/")) path = "/" + path;
            //force TLS connection
            return new URL("HTTPS", credential.getHostname(), path);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("This should never Happened! Protocols are locked to HTTPS and HTTP!", e);
        }
    }

    protected InputStream execute(String method, String path, APIEventBean acsParams, InputStream uploadStream, Long size) throws NetStorageException {
        try {
            return new NetStorageCMSv35Signer(
                    method,
                    this.getNetstorageUri(path),
                    acsParams,
                    uploadStream,
                    size != null && size > 0 ? size : -1,
                    this.getConnectTimeout(),
                    this.getReadTimeout()
            ).execute(this.credential);
        }
        catch (RequestSigningException ex) {
            try {
                return createRequestSigner(method, path, acsParams, uploadStream, size).execute(this.credential);
            } catch (RequestSigningException e) {
                throw new ConnectionException(e.getMessage(), e);
            }
        }
    }

    protected RequestSigner createRequestSigner(String method, String path, APIEventBean acsParams,
                                                InputStream uploadStream, Long size) {
        return new NetStorageCMSv35Signer(
                method,
                this.getNetstorageUri(path),
                acsParams,
                uploadStream,
                size != null && size > 0 ? size : -1,
                this.getConnectTimeout(),
                this.getReadTimeout()
        );
    }

    public NetStorageType getNetStorageType() throws NetStorageException {

        try {
            try (InputStream stream = stat("/")) {
                if (stream instanceof SignerInputStream) {
                    HttpURLConnection request = ((SignerInputStream) stream).getHttpRequest();
                    switch (request.getHeaderField("Server")) {
                        case "AkamaiNetStorage":
                            return NetStorageType.ObjectStore;
                        case "Apache":
                            return NetStorageType.FileStore;
                    }
                }
            }
        }
        catch (IOException ex) {
            throw new StreamClosingException("Exception while closing stream.", ex);
        }
        return NetStorageType.Unknown;
    }

    protected InputStream execute(String method, String path, APIEventBean acsParams) throws NetStorageException {
        return execute(method, path, acsParams, null, null);
    }

    public boolean delete(String path) throws NetStorageException {
        try (InputStream inputStream = execute("POST", path, new APIEventDelete())) {
            readToEnd(inputStream);
        }
        catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed.", e);
        }
        return true;
    }

    /**
     * list command
     * @return an inputstrwam (xml) of netstorage directories, files, symlinks.
     */
    public InputStream list(String path, Map<String, String> additionalParams) {
        APIEventBean apiEventBean = new APIEventList().withFormat("xml").withAdditionalParams(additionalParams);
        return execute("GET", path, apiEventBean);
    }

    public InputStream dir(String path) throws NetStorageException {
        return dir(path, "xml");
    }

    public InputStream dir(String path, String format) throws NetStorageException {
        return dir(path, format, null);
    }

    public InputStream dir(String path, String format, Map<String, String> additionalParams) throws NetStorageException {
        return execute("GET", path, new APIEventDir().withFormat(format).withAdditionalParams(additionalParams));
    }

    public InputStream download(String path) throws NetStorageException {
        return execute("GET", path, new APIEventDownload());
    }

    public InputStream du(String path) throws NetStorageException {
        return du(path, "xml");
    }

    public InputStream du(String path, String format) throws NetStorageException {
        return execute("GET", path, new APIEventDu().withFormat(format));
    }

    public boolean mkdir(String path) throws NetStorageException {
        try (InputStream inputStream = execute("PUT", path, new APIEventMkDir())) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed or read to end.", e);
        }
        return true;
    }

    public boolean mtime(String path) throws NetStorageException {
        return mtime(path, null);
    }

    public boolean mtime(String path, Date mtime) throws NetStorageException {
        //TODO: verify that this is for a file - cannot mtime on symlinks or dirs
        if (mtime == null)
            mtime = new Date();

        try (InputStream inputStream = execute("PUT", path, new APIEventMtime().withMtime(mtime))) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed or read to end.", e);
        }
        return true;
    }

    public boolean rename(String originalPath, String newPath) throws NetStorageException {
        //TODO: validate path and destination start with the same cpcode

        try (InputStream inputStream = execute("PUT", originalPath, new APIEventRename().to(newPath))) {
            readToEnd(inputStream);
        }
        catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed.", e);
        }
        return true;
    }

    public boolean rmdir(String path) throws NetStorageException {
        try (InputStream inputStream = execute("POST", path, new APIEventRmdir())) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed or read to end.", e);
        }
        return true;
    }

    public InputStream statIncludingImplicit(String path) throws NetStorageException {
        APIEventBean action = new APIEventStat().withFormat("xml");
        Map<String, String> additionalParams = new HashMap<>();
        additionalParams.put("implicit", "yes");
        action.withAdditionalParams(additionalParams);
        return execute("GET", path, action);
    }

    public InputStream stat(String path) throws NetStorageException {
        return stat(path, "xml");
    }

    public InputStream stat(String path, String format) throws NetStorageException {
        return execute("GET", path, new APIEventStat().withFormat(format));
    }

    public boolean symlink(String path, String target) throws NetStorageException {
        try (InputStream inputStream = execute("PUT", path, new APIEventSymlink().to(target))) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed or read to end.", e);
        }
        return true;
    }

    public boolean quickDelete(String path) throws NetStorageException {
        try (InputStream inputStream = execute("PUT", path, new APIEventQuickDelete())) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed or read to end.", e);
        }
        return true;
    }

    public boolean upload(String path, InputStream uploadFileStream) throws NetStorageException {
        return upload(path, uploadFileStream, null, new Date(), null, null, null, null, false);
    }

    public boolean upload(String path, InputStream uploadFileStream, Date mtime, Long size, byte[] md5Checksum, byte[] sha1Checksum, byte[] sha256Checksum, boolean indexZip) throws NetStorageException {
        return upload(path, uploadFileStream, null, mtime, size, md5Checksum, sha1Checksum, sha256Checksum, indexZip);
    }

    public boolean upload(String path, InputStream uploadFileStream, Map<String, String> additionalParams, Date mtime, Long size, byte[] md5Checksum, byte[] sha1Checksum, byte[] sha256Checksum, boolean indexZip) throws NetStorageException {

        // sanity check to ensure that indexZip is only true if the file destination is also a zip.
        // probably should throw an exception or warning instead.
        APIEventBean action =
                new APIEventUpload()
                        .withMtime(mtime)
                        .ofSize(size)
                        .withMd5(md5Checksum)
                        .withSha1(sha1Checksum)
                        .withSha256(sha256Checksum)
                        .isIndexZip(indexZip && path.endsWith(".zip"))
                        .withAdditionalParams(additionalParams);

        try (InputStream inputStream = execute("PUT", path, action, uploadFileStream, size)) {
            readToEnd(inputStream);
        } catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed.", e);
        }
        return true;
    }

    public boolean upload(String path, File srcFile) throws NetStorageException {
        return this.upload(path, srcFile, null, false);
    }

    public boolean upload(String path, File srcFile, Map<String, String> additionalParams) throws NetStorageException {
        return this.upload(path, srcFile, additionalParams, false);
    }

    public boolean upload(String path, File srcFile, boolean indexZip) throws NetStorageException {
        return upload(path, srcFile, null, indexZip);
    }

    public boolean upload(String path, File srcFile, Map<String, String> additionalParams, boolean indexZip) throws NetStorageException {
        if (!srcFile.exists())
            throw new LocalFileNotFoundException(String.format("Src file is not accessible %s", srcFile.toString()));

        Date mTime = new Date(srcFile.lastModified());
        byte[] checksum;
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(srcFile))) {
            checksum = Utils.computeHash(inputStream, Utils.HashAlgorithm.SHA256);

            try (InputStream uploadInputStream = new BufferedInputStream(new FileInputStream(srcFile))) {
                long size = srcFile.length();
                return this.upload(path, uploadInputStream, additionalParams, mTime, size, null, null, checksum, indexZip);
            } catch (FileNotFoundException e) {
                throw new LocalFileNotFoundException("Source file not found.", e);
            } catch (IOException e) {
                throw new StreamClosingException("Could not auto-close file upload input stream.", e);
            }
        } catch (FileNotFoundException e) {
            throw new LocalFileNotFoundException("Source file not found while caculating checksum.", e);
        } catch (IOException e) {
            throw new StreamClosingException("Could not auto-close file upload input stream for calculation of checksum.", e);
        }
    }

    public boolean setmd(String path, Map<String, String> additionalParams) throws NetStorageException {

        try (InputStream inputStream = execute("PUT", path, new APIEventSetmd().withAdditionalParams(additionalParams))) {
            readToEnd(inputStream);
        }
        catch (IOException e) {
            throw new StreamClosingException("Response could not be auto closed.", e);
        }
        return true;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

}
