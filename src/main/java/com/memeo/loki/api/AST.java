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

package com.memeo.loki.api;

import net.liftweb.json.JsonAST;
import net.liftweb.json.JsonParser;
import scala.collection.convert.WrapAsJava$;
import scala.collection.convert.WrapAsScala$;
import scala.collection.convert.Wrappers;
import scala.collection.convert.Wrappers$;
import scala.util.parsing.json.JSONType;
import sun.security.util.BigInt;

import java.math.BigInteger;
import java.util.*;

public class AST
{
    public static interface Value<T>
    {
        T get();

        JsonAST.JValue toJValue();
    }

    public static interface MutableValue<T> extends Value<T>
    {
        void set(T value);
    }

    static abstract class ValueImpl<T> implements Value<T>
    {
        private final T value;

        ValueImpl(T value)
        {
            this.value = value;
        }

        @Override
        public final T get()
        {
            return value;
        }
    }

    static abstract class MutableValueImpl<T> implements MutableValue<T>
    {
        private T value;

        MutableValueImpl(T value)
        {
            this.value = value;
        }

        @Override
        public T get()
        {
            return value;
        }

        @Override
        public final void set(T value)
        {
            this.value = value;
        }
    }

    public static final class NothingValue implements Value<Object>
    {
        private NothingValue() {}
        private static final NothingValue NOTHING = new NothingValue();

        @Override
        public Object get() {
            return null;
        }

        @Override
        public JsonAST.JValue toJValue() {
            return JsonAST.JNothing$.MODULE$;
        }
    }

    public static final class NullValue implements Value<Object>
    {
        public static final NullValue NULL = new NullValue();

        private NullValue()
        {
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public JsonAST.JValue toJValue() {
            return JsonAST.JNull$.MODULE$;
        }
    }

    public static final class BooleanValue extends ValueImpl<Boolean>
    {
        public BooleanValue(Boolean value)
        {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue()
        {
            return new JsonAST.JBool(get());
        }
    }

    public static final class MutableBooleanValue extends MutableValueImpl<Boolean>
    {
        public MutableBooleanValue(Boolean value)
        {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue()
        {
            return new JsonAST.JBool(get());
        }
    }

    public static final class DoubleValue extends ValueImpl<Double>
    {
        public DoubleValue(Double value) {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue() {
            return new JsonAST.JDouble(get());
        }
    }

    public static final class MutableDoubleValue extends MutableValueImpl<Double>
    {
        public MutableDoubleValue(Double value) {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue() {
            return new JsonAST.JDouble(get());
        }
    }

    public static final class IntValue extends ValueImpl<BigInteger>
    {
        public IntValue(BigInteger value) {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue() {
            return new JsonAST.JInt(scala.math.BigInt.apply(get()));
        }
    }

    public static final class MutableIntValue extends MutableValueImpl<BigInteger>
    {
        public MutableIntValue(BigInteger value) {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue() {
            return new JsonAST.JInt(scala.math.BigInt.apply(get()));
        }
    }

    public static final class StringValue extends ValueImpl<String>
    {
        public StringValue(String value)
        {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue()
        {
            return new JsonAST.JString(get());
        }
    }

    public static class MutableStringValue extends MutableValueImpl<String>
    {
        public MutableStringValue(String value)
        {
            super(value);
        }

        @Override
        public JsonAST.JValue toJValue() {
            return new JsonAST.JString(get());
        }
    }

    public static final class ArrayValue extends ValueImpl<List<Value<?>>>
    {
        public ArrayValue(List<Value<?>> value)
        {
            super(Collections.unmodifiableList(value));
        }

        @Override
        public JsonAST.JValue toJValue()
        {
            List<JsonAST.JValue> jvalues = new AbstractList<JsonAST.JValue>()
            {
                @Override
                public JsonAST.JValue get(int index)
                {
                    return ArrayValue.this.get().get(index).toJValue();
                }

                @Override
                public int size() {
                    return ArrayValue.this.get().size();
                }
            };
            return new JsonAST.JArray(WrapAsScala$.MODULE$.asScalaBuffer(jvalues).toList());
        }
    }

    public static final class ObjectValue extends ValueImpl<Map<String, Value<?>>>
    {
        public ObjectValue(Map<String, Value<?>> value)
        {
            super(Collections.unmodifiableMap(value));
        }

        public ObjectValue(JsonAST.JObject obj)
        {
            super(value(obj));
        }

        private static Map<String, Value<?>> value(JsonAST.JObject obj)
        {
            Collection<JsonAST.JField> jFields = WrapAsJava$.MODULE$.asJavaCollection(((JsonAST.JObject) obj).obj());
            Map<String, Value<?>> map = new LinkedHashMap<>();
            for (JsonAST.JField jField : jFields)
                map.put(jField.name(), of(jField.value()));
            return map;
        }

        @Override
        public JsonAST.JValue toJValue()
        {
            Set<JsonAST.JField> entries = new AbstractSet<JsonAST.JField>()
            {
                @Override
                public Iterator<JsonAST.JField> iterator()
                {
                    final Iterator<Map.Entry<String, Value<?>>> it = ObjectValue.this.get().entrySet().iterator();
                    return new Iterator<JsonAST.JField>()
                    {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public JsonAST.JField next() {
                            Map.Entry<String, Value<?>> e = it.next();
                            return new JsonAST.JField(e.getKey(), e.getValue().toJValue());
                        }

                        @Override
                        public void remove() {
                            it.remove();
                        }
                    };
                }

                @Override
                public int size()
                {
                    return ObjectValue.this.get().size();
                }
            };
            return new JsonAST.JObject(WrapAsScala$.MODULE$.asScalaSet(entries).toList());
        }
    }

    public static Value<?> fromJson(String json)
    {
        return of(JsonParser.parse(json));
    }

    public static Value<?> of(JsonAST.JValue value)
    {
        if (value == JsonAST.JNothing$.MODULE$)
            return NothingValue.NOTHING;
        if (value == JsonAST.JNull$.MODULE$)
            return NullValue.NULL;
        if (value instanceof JsonAST.JBool)
            return new BooleanValue(((JsonAST.JBool) value).values());
        if (value instanceof JsonAST.JInt)
            return new IntValue(((JsonAST.JInt) value).num().bigInteger());
        if (value instanceof JsonAST.JDouble)
            return new DoubleValue(((JsonAST.JDouble) value).num());
        if (value instanceof JsonAST.JString)
            return new StringValue(((JsonAST.JString) value).values());
        if (value instanceof JsonAST.JArray)
        {
            Collection<JsonAST.JValue> jValues = WrapAsJava$.MODULE$.asJavaCollection(((JsonAST.JArray) value).arr());
            List<Value<?>> valueList = new ArrayList<Value<?>>(jValues.size());
            for (JsonAST.JValue jvalue : jValues)
                valueList.add(of(jvalue));
            return new ArrayValue(valueList);
        }
        if (value instanceof JsonAST.JObject)
        {
            Collection<JsonAST.JField> jFields = WrapAsJava$.MODULE$.asJavaCollection(((JsonAST.JObject) value).obj());
            Map<String, Value<?>> map = new LinkedHashMap<String, Value<?>>();
            for (JsonAST.JField jField : jFields)
                map.put(jField.name(), of(jField.value()));
            return new ObjectValue(map);
        }
        throw new IllegalArgumentException("can't convert JSON value " + value);
    }
}
