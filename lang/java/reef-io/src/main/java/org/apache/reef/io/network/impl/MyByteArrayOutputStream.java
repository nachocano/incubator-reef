package org.apache.reef.io.network.impl;

import java.io.ByteArrayOutputStream;

public class MyByteArrayOutputStream extends ByteArrayOutputStream {

  @Override
  public synchronized byte toByteArray()[] {
    return buf;
  }

}
