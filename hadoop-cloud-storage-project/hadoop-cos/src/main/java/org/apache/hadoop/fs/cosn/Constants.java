/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

/**
 * constant definition.
 */
public final class Constants {
  private Constants() {
  }

  public static final String BLOCK_TMP_FILE_PREFIX = "cos_";
  public static final String BLOCK_TMP_FILE_SUFFIX = "_local_block";

  // The maximum number of files listed in a single COS list request.
  public static final int COS_MAX_LISTING_LENGTH = 999;

  // The maximum number of parts supported by a multipart uploading.
  public static final int MAX_PART_NUM = 10000;

  // The maximum size of a part
  public static final long MAX_PART_SIZE = (long) 2 * Unit.GB;
  // The minimum size of a part
  public static final long MIN_PART_SIZE = (long) Unit.MB;

  public static final String COSN_SECRET_ID_ENV = "COSN_SECRET_ID";
  public static final String COSN_SECRET_KEY_ENV = "COSN_SECRET_KEY";

  // The committer constants
  public static final String BUFFER_DIR = "fs.cos.buffer.dir";
  public static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";

  public static final String COS_METADATA_STORE_IMPL =
          "fs.cos.metadatastore.impl";

  public static final String METADATASTORE_AUTHORITATIVE =
          "fs.cos.metadatastore.authoritative";

  public static final String AUTHORITATIVE_PATH = "fs.cos.authoritative.path";

  /**
   * How long to wait for the thread pool to terminate when cleaning up.
   * Value: {@value} seconds.
   */
  public static final int THREAD_POOL_SHUTDOWN_DELAY_SECONDS = 30;

  // size of each of or multipart pieces in bytes
  public static final String MULTIPART_SIZE = "fs.cosn.block.size";
  public static final long DEFAULT_MULTIPART_SIZE = 8 * Unit.MB; // 64M

}
