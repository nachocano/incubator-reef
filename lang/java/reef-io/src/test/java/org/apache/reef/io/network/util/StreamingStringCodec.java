/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.io.network.util;

import org.apache.reef.io.network.impl.StreamingCodec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class StreamingStringCodec implements StreamingCodec<String> {
  @Override
  public byte[] encode(final String obj) {
    return obj.getBytes();
  }

  @Override
  public String decode(final byte[] buf) {
    return new String(buf);
  }

  @Override
  public void encodeToStream(final String obj, final DataOutputStream stream) {
    try {
      stream.writeUTF(obj);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String decodeFromStream(final DataInputStream stream) {
    try {
      return stream.readUTF();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int nonUTFSizeToEncodeToStream(final String obj, final DataOutputStream stream) {
    encodeToStream(obj, stream);
    return 0;
  }
}
