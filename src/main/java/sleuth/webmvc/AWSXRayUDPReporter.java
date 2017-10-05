package sleuth.webmvc;

import com.squareup.moshi.JsonWriter;
import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.LinkedHashMap;
import java.util.Map;
import okio.Buffer;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

class AWSXRayUDPReporter implements Reporter<Span>, Closeable {
  static final int PACKET_LENGTH = 256 * 1024;
  static final ThreadLocal<byte[]> BUF = new ThreadLocal<byte[]>() {
    @Override protected byte[] initialValue() {
      return new byte[PACKET_LENGTH];
    }
  };

  final InetSocketAddress address;
  final DatagramSocket socket;

  AWSXRayUDPReporter() {
    address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 2000);
    try {
      socket = new DatagramSocket();
    } catch (SocketException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override public void report(Span span) {
    DatagramPacket packet = new DatagramPacket(BUF.get(), PACKET_LENGTH, address);
    try {
      packet.setData(toUDPMessage(span));
      socket.send(packet);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override public void close() throws IOException {
    socket.close();
  }

  static byte[] toUDPMessage(Span span) throws IOException {
    Buffer buffer = new Buffer();
    buffer.writeUtf8("{\"format\": \"json\", \"version\": 1}\n");
    JsonWriter writer = JsonWriter.of(buffer);
    writer.beginObject();
    writer.name("trace_id").value(new StringBuilder()
        .append("1-")
        .append(span.traceId(), 0, 8)
        .append('-')
        .append(span.traceId(), 8, 32).toString());
    if (span.parentId() != null) writer.name("parent_id").value(span.parentId());
    writer.name("id").value(span.id());
    if (span.kind() == null
        || span.kind() != Span.Kind.SERVER && span.kind() != Span.Kind.CONSUMER) {
      writer.name("type").value("subsegment");
      if (span.kind() != null) writer.name("namespace").value("remote");
    }
    writer.name("name").value(span.localServiceName());
    if (span.timestamp() != null) {
      writer.name("start_time").value(span.timestamp() / 1_000_000.0D);
      if (span.duration() != null) {
        writer.name("end_time").value((span.timestamp() + span.duration()) / 1_000_000.0D);
      } else {
        writer.name("in_progress").value(true);
      }
    }

    String httpRequestMethod = null, httpRequestUrl = null;
    Integer httpResponseStatus = null;
    boolean http = false;

    Map<String, String> annotations = new LinkedHashMap<>();
    Map<String, String> metadata = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : span.tags().entrySet()) {
      if (entry.getKey().startsWith("http.")) {
        http = true;
        switch (entry.getKey()) {
          case "http.method":
            httpRequestMethod = entry.getValue();
            continue;
          case "http.url":
            httpRequestUrl = entry.getValue();
            continue;
          case "http.status_code":
            httpResponseStatus = Integer.parseInt(entry.getValue());
            continue;
        }
      }
      String key = entry.getKey().replace('.', '_');
      if (entry.getValue().length() < 250) {
        annotations.put(key, entry.getValue());
      } else {
        metadata.put(key, entry.getValue());
      }
    }

    if (http) {
      if (httpRequestMethod == null) {
        httpRequestMethod = span.name(); // TODO validate
      }
      writer.name("http");
      writer.beginObject();
      if (httpRequestMethod != null || httpRequestUrl != null) {
        writer.name("request");
        writer.beginObject();
        if (httpRequestMethod != null) {
          writer.name("method").value(httpRequestMethod.toUpperCase());
        }
        if (httpRequestUrl != null) {
          writer.name("url").value(httpRequestUrl);
        }
        writer.endObject();
      }
      if (httpResponseStatus != null) {
        writer.name("response");
        writer.beginObject();
        writer.name("status").value(httpResponseStatus);
        writer.endObject();
      }
      writer.endObject();
    }

    if (!annotations.isEmpty()) {
      writer.name("annotations");
      writer.beginObject();
      if (httpRequestMethod != null && span.name() != null && !httpRequestMethod.equals(
          span.name())) {
        writer.name("operation").value(span.name());
      }
      for (Map.Entry<String, String> annotation : annotations.entrySet()) {
        writer.name(annotation.getKey()).value(annotation.getValue());
      }
      writer.endObject();
    }
    if (!metadata.isEmpty()) {
      writer.name("metadata");
      writer.beginObject();
      for (Map.Entry<String, String> metadatum : metadata.entrySet()) {
        writer.name(metadatum.getKey()).value(metadatum.getValue());
      }
      writer.endObject();
    }
    writer.endObject();
    return buffer.readByteArray();
  }
}