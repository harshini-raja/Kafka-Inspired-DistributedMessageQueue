package com.distributedQueue.Adapters;

import java.io.IOException;
import java.time.Instant;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class InstantTypeAdapter  extends TypeAdapter<Instant> {

    @Override
    public void write(JsonWriter out, Instant value) throws IOException {
        if (value == null) {
            out.nullValue();
        } else {
            out.value(value.toString()); // Serialize Instant to ISO-8601 string
        }
    }

    @Override
    public Instant read(JsonReader in) throws IOException {
        String value = in.nextString();
        return Instant.parse(value); // Deserialize ISO-8601 string to Instant
    }
}
