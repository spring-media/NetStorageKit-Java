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
package com.akamai.netstorage.cli;

import com.akamai.netstorage.DefaultCredential;
import com.akamai.netstorage.NetStorage;
import com.akamai.netstorage.Utils;
import com.akamai.netstorage.exception.NetStorageException;

import java.io.*;
import java.util.Properties;

/**
 * Command Line sample application to demonstrate the utilization of the
 * NetstorageKit. This can be used for both command line invocation or reference
 * on how to leverage the Kit. All supported commands are implemented in this
 * sample for convience.
 *
 * @author colinb@akamai.com (Colin Bendell)
 */
public class CMS {
    /**
     * Implement the command-line client.
     * @see CMS
     * @param args Command line.
     * @throws Exception generic exception
     */
    public static void main(String[] args) throws Exception {
        String action = null;
        String user = null;
        String key = null;
        String netstorageURI = null;
        String uploadfile = null;
        String outputfile = null;
        String targetFilename = null;
        String dstFilename = null;
        int connectTimeout = 10000;
        int readTimeout = 10000;
        boolean indexZip = false;


        String firstarg = null;
        for (String arg : args)
            if (firstarg != null) {
                switch (firstarg) {
                    case "-h":
                        help();
                        return;
                    case "-a":
                        action = arg;
                        break;
                    case "-u":
                        user = arg;
                        break;
                    case "-k":
                        key = arg;
                        break;
                    case "-o":
                        outputfile = arg;
                        break;
                    case "-f":
                        uploadfile = arg;
                        break;
                    case "-t":
                        targetFilename = arg;
                        break;
                    case "-d":
                        dstFilename = arg;
                        break;
                    case "-c":
                        connectTimeout = Integer.parseInt(arg);
                    case "-r":
                        readTimeout = Integer.parseInt(arg);
                        break;
                }
                firstarg = null;
            } else if (arg.equals("-indexzip"))
                indexZip = true;
            else if (!arg.startsWith("-"))
                netstorageURI = arg;
            else
                firstarg = arg;

        try {
            execute(action, user, key, netstorageURI, uploadfile, outputfile, targetFilename, dstFilename, indexZip, connectTimeout,readTimeout);
        } catch (NetStorageException e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    public static void execute(String action, String user, String key, String netstorageURI,
                        String uploadfile, String outputfile, String target, String dst, boolean indexZip,
                        int connectTimeout, int readTimeout) throws NetStorageException, IOException {

        String host = null;
        String path = netstorageURI;
        if (netstorageURI == null || user == null || key == null) {

            File credsFile = new File(System.getProperty("user.home") + File.separator + ".edgerc");
            String section = "netstorage";
            Properties props = Utils.readIniSection(credsFile, section);
            host = props.getProperty(DefaultCredential.HOSTNAME_PROPERTY);
            user = props.getProperty(DefaultCredential.USERNAME_PROPERTY);
            key = props.getProperty(DefaultCredential.KEY_PROPERTY);
        }

        if (action == null || netstorageURI == null || user == null || key == null) {
            help();
            return;
        }

        if (host == null) {
            String[] hostpath = netstorageURI.split("/", 2);
            host = hostpath[0];
            path = "/" + hostpath[1];
        }
        NetStorage ns = new NetStorage(new DefaultCredential(host, user, key), connectTimeout, readTimeout);
        InputStream result = null;
        boolean success = true;

        switch (action)
        {
            case "delete":
                success = ns.delete(path);
                break;
            case "dir":
                result = ns.dir(path);
                break;
            case "download":
                result = ns.download(path);
                break;
            case "du":
                result = ns.du(path);
                break;
            case "mkdir":
                success = ns.mkdir(path);
                break;
            case "mtime":
                success = ns.mtime(path);
                break;
            case "rename":
                if (dst == null) {
                    help();
                    return;
                }
                success = ns.rename(path, dst);
                break;
            case "rmdir":
                success = ns.rmdir(path);
                break;
            case "stat":
                result = ns.stat(path);
                break;
            case "symlink":
                if (target == null) {
                    help();
                    return;
                }
                success = ns.symlink(path, target);
                break;
            case "upload":
                if (uploadfile == null) {
                    help();
                    return;
                }
                success = ns.upload(path, new File(uploadfile), null, indexZip);
                break;
            default:
                help();
                return;
        }

        if (result != null) {
            OutputStream output;
			boolean usingStdOut;

			if (outputfile != null) {
				output = new FileOutputStream(outputfile);
				usingStdOut = false;
			} else {
				output = System.out;
				usingStdOut = true;
			}

            try {
                byte[] buffer = new byte[1024 * 1024];
                for (int length; (length = result.read(buffer)) > 0; ) {
                    output.write(buffer, 0, length);
                }
                output.flush();
            } finally {
            	if (!usingStdOut){
            		output.close();
            	}
                result.close();
            }
        }
        else if (success)
            System.out.println("Success.");
        else
            System.err.println("Error.");
    }

    static void help()
    {
        System.err.println("\n"
                + "Usage: cms <-a action> <-u user> <-k key>\n"
                + "[-o outfile] [-f srcfile]\n"
                + "[-t targetpath] [-d newpath]\n"
                + "[-c connectTimeout] [-r readTimeout]\n"
                + "<-indexzip> <host/path>\n"
                + "\n"
                + "Where:\n"
                + "action          one of: delete, dir, download, du, mkdir, mtime, rename, rmdir, stat, symlink, upload\n"
                + "user            username defined in the Luna portal\n"
                + "key             unique key used to sign api requests\n"
                + "outfile         local file name to write when action=download\n"
                + "srcfile         local file used as source when action=upload\n"
                + "connectTimeout  http connect timeout in milliseconds\n"
                + "readTimeout     http read timeout in milliseconds - useful when uploading via proxy\n"
                + "targetpath      the absolute path (/1234/example.jpg) pointing to the existing target when action=symlink\n"
                + "newpath         the absolute path (/1234/example.jpg) for the new file when action=rename\n"
                + "host/path       the netstorage hostname and path to the file being manipulated (example.akamaihd.net/1234/example.jpg)\n"
                + "\n\n Example: cms -a dir -u user1 -k 1234abcd example.akamaihd.net/1234\n"
                + "\n\n");
    }
}
