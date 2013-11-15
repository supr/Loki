/*
 * Copyright 2013 Memeo, Inc.
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

package com.memeo.loki

import com.memeo.loki.api
import java.util
import javax.script._
import net.liftweb.json.Printer.compact
import net.liftweb.json.render

object Javascript {
  val engine = new ScriptEngineManager().getEngineByName("javascript")
}

class JavascriptMapAdapter(mapFunc:String) extends api.MapFunction
{
  val script = f"var thedoc = JSON.parse(documentJson);\n" +
    f"var emit = function(key, value) {\n" +
    f"  var k = com.memeo.loki.AST.fromJson(JSON.stringify(key));\n" +
    f"  var v = com.memeo.loki.AST.NothingValue.NOTHING;\n" +
    f"  if (value != null) {\n" +
    f"    v = com.memeo.loki.AST.fromJson(JSON.stringify(value));\n" +
    f"  }\n" +
    f"  emitter.emit(key, value);\n" +
    f"}\n" +
    f"var mapfun = $mapFunc;\n" +
    f"mapfun(thedoc);"

  override def map(doc:api.AST.ObjectValue, emitter:api.RowEmitter):Unit = {
    val bindings = Javascript.engine.createBindings()
    bindings.put("documentJson", compact(render(doc.toJValue)))
    bindings.put("emitter", emitter)
    Javascript.engine.eval(script, bindings)
  }
}

class JavascriptReduceAdapter(reduceFunc:String) extends api.ReduceFunction
{
  val script = f"var reducefun = $reduceFunc;\n" +
    f"var result = JSON.stringify(reducefun(JSON.parse(keys), JSON.parse(values), rereduce));"

  override def reduce(keys: util.List[api.AST.Value[_]], values: util.List[api.AST.Value[_]], rereduce: Boolean): api.AST.Value[_] = {
    val bindings = Javascript.engine.createBindings()
    keys match {
      case null => bindings.put("keys", "\"null\"")
      case _ => bindings.put("keys", compact(render(new api.AST.ArrayValue(keys).toJValue)))
    }
    bindings.put("values", compact(render(new api.AST.ArrayValue(values).toJValue)))
    bindings.put("rereduce", rereduce)
    Javascript.engine.eval(script, bindings)
    api.AST.fromJson(bindings.get("result").asInstanceOf[String])
  }
}