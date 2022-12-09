/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;


public class StateDescriptorSerializationProxy extends VersionedIOReadableWritable {

	public static final int VERSION = 1;

	private OperatorID operatorID;

	private Collection<StateDescriptor> stateDescriptors;

	private ClassLoader userCodeClassLoader;

	@Nullable
	private TypeSerializer keySerializer;

	public StateDescriptorSerializationProxy(
		OperatorID operatorID, Collection<StateDescriptor> stateDescriptors,
		@Nullable TypeSerializer keySerializer) {
		Preconditions.checkArgument(
			operatorID!= null && stateDescriptors != null);
		this.operatorID = operatorID;
		this.stateDescriptors = stateDescriptors;
		this.keySerializer = keySerializer;
	}

	public StateDescriptorSerializationProxy(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);
		byte[] operatorIDBuffer = operatorID.getBytes();
		out.writeInt(operatorIDBuffer.length);
		out.write(operatorIDBuffer);
		byte[] stateDescBuffer = InstantiationUtil.serializeObject(stateDescriptors);
		out.writeInt(stateDescBuffer.length);
		out.write(stateDescBuffer);
		if (keySerializer != null) {
			out.writeBoolean(true);
			byte[] keySerializerBuffer = InstantiationUtil.serializeObject(keySerializer);
			out.writeInt(keySerializerBuffer.length);
			out.write(keySerializerBuffer);
		} else {
			out.writeBoolean(false);
		}
	}


	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);
		int operatorIDBufferSize = in.readInt();
		byte[] operatorIDBuffer = new byte[operatorIDBufferSize];
		in.read(operatorIDBuffer);
		operatorID = new OperatorID(operatorIDBuffer);
		int stateDescBufferSize = in.readInt();
		byte[] stateDescBuffer = new byte[stateDescBufferSize];
		in.read(stateDescBuffer);
		try {
			stateDescriptors = InstantiationUtil.deserializeObject(stateDescBuffer, userCodeClassLoader);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		boolean isKeyState = in.readBoolean();
		if (isKeyState) {
			int keySerializerBufferSize = in.readInt();
			byte[] keySerializerBuffer = new byte[keySerializerBufferSize];
			in.read(keySerializerBuffer);
			try {
				keySerializer = InstantiationUtil.deserializeObject(keySerializerBuffer, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public Collection<StateDescriptor> getStateDescriptors() {
		return stateDescriptors;
	}

	public OperatorID getOperatorID() {
		return operatorID;
	}

	@Nullable
	public TypeSerializer getKeySerializer() {
		return keySerializer;
	}
}
