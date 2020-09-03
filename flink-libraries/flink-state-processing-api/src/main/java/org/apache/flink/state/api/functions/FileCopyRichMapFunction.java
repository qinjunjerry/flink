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

package org.apache.flink.state.api.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * This mapper copies files from an existing savepoint into a new directory.
 */
public final class FileCopyRichMapFunction extends RichMapFunction<String, String> {

	private static final long serialVersionUID = 1L;

	// the destination path to copy file
	private final String path;

	public FileCopyRichMapFunction(String path) {
		this.path = Preconditions.checkNotNull(path, "The destination path cannot be null");
	}

	@Override
	public void open(Configuration configuration) throws Exception {
		// create the parent dir only in the first subtask
		if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
			Path destPath = new Path(path);
			destPath.getFileSystem().mkdirs(destPath);
		}
	}

	@Override
	public String map(String sourceFile) throws Exception {
		Files.copy(
			Paths.get(sourceFile), // source file
			Paths.get(path, Paths.get(sourceFile).getFileName().toString()) // destination file
		);
		return sourceFile;
	}
}
