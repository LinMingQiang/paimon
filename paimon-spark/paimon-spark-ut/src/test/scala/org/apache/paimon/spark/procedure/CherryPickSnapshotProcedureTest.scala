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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

import java.util

/** ITCase for [[CherryPickSnapshotProcedure ]]. */
class CherryPickSnapshotProcedureTest extends PaimonSparkTestBase {

  test("Paimon Procedure: cherry-pick snapshot from branch into main branch.") {
    createBranch(true, coreOptions)
    val query = () => spark.sql("SELECT * FROM T ")
    var mainTable = loadTable("T")
    val branchTable = loadTable("T$branch_test")
    assert(branchTable.snapshotManager().latestSnapshotId() == 1)
    spark.sql("INSERT INTO `T$branch_test` VALUES (1, 'branch-apple', 'pt')")
    assert(branchTable.snapshotManager().latestSnapshotId() == 2)
    assert(mainTable.snapshotManager().latestSnapshotId() == 1)
    checkAnswer(query(), Row(1, "apple", "pt"))

    checkAnswer(
      spark.sql("CALL paimon.sys.cherry_pick(table => 'test.T', branch => 'test', snapshot => 2)"),
      Row("Cherry-pick to snapshotID : 2"))
    mainTable = loadTable("T")
    assert(mainTable.snapshotManager().latestSnapshotId() == 2)

    checkAnswer(query(), Row(1, "branch-apple", "pt"))
  }

  def createBranch(primaryTable: Boolean, options: util.Map[String, String]): Unit = {
    val sb = new StringBuilder
    options.forEach((k: String, v: String) => sb.append(String.format(",'%s'='%s'", k, v)))
    spark.sql(s"""
                 |CREATE TABLE T (k INT, v STRING, pt STRING)
                 |TBLPROPERTIES (
                 | ${if (primaryTable) "'primary-key'='k,pt'," else ""}
                 | ${sb.substring(1, sb.toString.length)})
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES" + " (1, 'apple', 'pt')")

    spark.sql("CALL paimon.sys.create_tag(table => 'test.T', tag => 'tag1', snapshot => 1)")

    spark.sql("CALL paimon.sys.create_branch(table => 'test.T', branch => 'test', tag => 'tag1')")

  }

  def coreOptions: util.Map[String, String] = {
    val options = new util.HashMap[String, String]
    options.put("bucket", "1")
    options.put("write-only", "true")
    options.put("merge-engine", "partial-update")
    options.put("changelog-producer", "input")
    options
  }
}
