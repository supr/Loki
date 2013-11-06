package com.memeo.loki

import org.mapdb.DBMaker
import java.io.File

/**
 * Copyright (C) 2013 Memeo, Inc.
 * All Rights Reserved
 */
class MapDBTest
{
  val db = DBMaker.newFileDB(new File("test.db")).make()
  val maker = db.createTreeMap("_main")
  val m = maker.nodeSize(8).valuesStoredOutsideNodes(true).makeOrGet[String, String]()
  m.put("_revision", "1")
  db.commit()
  db.close()
  val db2 = DBMaker.newFileDB(new File("test.db")).readOnly().make()
  val m2 = db2.getTreeMap("_main")
  print(m2.entrySet())
  db2.close()
}
