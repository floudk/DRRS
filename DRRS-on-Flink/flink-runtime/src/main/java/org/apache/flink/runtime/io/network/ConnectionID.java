/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;
import java.net.InetSocketAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link ConnectionID} identifies a connection to a remote task manager by the socket address and
 * a connection index. This allows multiple connections to the same task manager to be distinguished
 * by their connection index.
 *
 * <p>The connection index is assigned by the {@link IntermediateResult} and ensures that it is safe
 * to multiplex multiple data transfers over the same physical TCP connection.
 */
public class ConnectionID implements Serializable {

    private static final long serialVersionUID = -8068626194818666857L;

    private final InetSocketAddress address;

    private final int connectionIndex;

    private final ResourceID resourceID;

    public ConnectionID(TaskManagerLocation connectionInfo, int connectionIndex) {
        this(
                connectionInfo.getResourceID(),
                new InetSocketAddress(connectionInfo.address(), connectionInfo.dataPort()),
                connectionIndex);
    }

    public ConnectionID(ResourceID resourceID, InetSocketAddress address, int connectionIndex) {
        this.resourceID = checkNotNull(resourceID);
        this.address = checkNotNull(address);
        checkArgument(connectionIndex >= 0);
        this.connectionIndex = connectionIndex;
    }

    public ResourceID getResourceID() {
        return resourceID;
    }

    public InetSocketAddress getAddress() {
        return address;
    }

    public int getConnectionIndex() {
        return connectionIndex;
    }

    @Override
    public int hashCode() {
        return address.hashCode() + (31 * connectionIndex);
    }

    @Override
    public boolean equals(Object other) {
        if (other.getClass() != ConnectionID.class) {
            return false;
        }

        final ConnectionID ra = (ConnectionID) other;
        return ra.getAddress().equals(address)
                && ra.getConnectionIndex() == connectionIndex
                && ra.getResourceID().equals(resourceID);
    }

    @Override
    public String toString() {
        return String.format(
                "%s (%s) [%s]", address, resourceID.getStringWithMetadata(), connectionIndex);
    }

    // ------------------------------------------------------------------------
    // Scale Utils
    // ------------------------------------------------------------------------
    public static ConnectionID createScaleConnectionID(TaskManagerLocation connectionInfo, int port){
        return new ConnectionID(
                connectionInfo.getResourceID(),
                new InetSocketAddress(connectionInfo.address(), port),
                0);
    }
}
