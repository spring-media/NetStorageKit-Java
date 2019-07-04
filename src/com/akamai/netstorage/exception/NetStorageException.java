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
package com.akamai.netstorage.exception;


import java.util.List;
import java.util.Map;

/**
 * Netstorage Exceptions are intended to differentiate between IO (Networking and File) exceptions
 *
 * @author colinb@akamai.com (Colin Bendell)
 */
public abstract class NetStorageException extends RuntimeException {

	private static final long serialVersionUID = 5716437270940718895L;

    public NetStorageException(String message) {
        super(message);
    }

    public NetStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NetStorageException from(int responseCode, String responseMessage, Map<String, List<String>> headerFields) {
        switch (responseCode) {
            case 404 : return new FileNotFoundException(responseMessage);
            case 403 : return new AccessForbiddenException(responseMessage);
            case 400 : return new IllegalArgumentException(responseMessage);
        }
        return new UnspecificNetstorageException(responseCode, responseMessage);
    }
}
