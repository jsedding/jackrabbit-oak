/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.spi.security;

import org.apache.jackrabbit.oak.security.internal.SecurityProviderRegistration;

/**
 * Holds the names of well-known registration properties for security-related components
 *
 */
public abstract class RegistrationConstants {

    /**
     * Name to be used when registering components that are required by the {@link SecurityProviderRegistration}
     */
    public static final String OAK_SECURITY_NAME = "oak.security.name";
    
    private RegistrationConstants() {
        
    }
}
