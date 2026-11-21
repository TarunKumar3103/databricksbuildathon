// Minimal Parquet writer: one row group per file, one data page (v1) per column,
// PLAIN values, optional dictionary pages, no compression. Enough for flat tables of
// INT32 / INT64 / DATE / STRING columns, required or optional.
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace pq {

enum PhysType { T_INT32 = 1, T_INT64 = 2, T_BYTE_ARRAY = 6 };
enum { ENC_PLAIN = 0, ENC_RLE = 3, ENC_RLE_DICTIONARY = 8 };

struct Buf {
  std::vector<uint8_t> d;
  void u8(uint8_t x) { d.push_back(x); }
  void bytes(const void* p, size_t n) {
    const uint8_t* c = (const uint8_t*)p;
    d.insert(d.end(), c, c + n);
  }
  void varint(uint64_t x) {
    while (x >= 0x80) {
      d.push_back(uint8_t(x) | 0x80);
      x >>= 7;
    }
    d.push_back(uint8_t(x));
  }
  void zz32(int32_t x) { varint((uint32_t)(((uint32_t)x << 1) ^ (uint32_t)(x >> 31))); }
  void zz64(int64_t x) { varint((uint64_t)(((uint64_t)x << 1) ^ (uint64_t)(x >> 63))); }
  void le32(uint32_t x) { bytes(&x, 4); }
  size_t size() const { return d.size(); }
};

// Thrift compact protocol.
struct TC {
  Buf& b;
  std::vector<int16_t> stack;
  int16_t last = 0;
  explicit TC(Buf& out) : b(out) {}
  void field(int16_t id, uint8_t type) {
    int delta = id - last;
    if (delta > 0 && delta <= 15)
      b.u8(uint8_t((delta << 4) | type));
    else {
      b.u8(type);
      b.zz32(id);
    }
    last = id;
  }
  void i32(int16_t id, int32_t v) {
    field(id, 5);
    b.zz32(v);
  }
  void i64(int16_t id, int64_t v) {
    field(id, 6);
    b.zz64(v);
  }
  void str(int16_t id, const std::string& s) {
    field(id, 8);
    b.varint(s.size());
    b.bytes(s.data(), s.size());
  }
  void sbegin(int16_t id) {
    field(id, 12);
    stack.push_back(last);
    last = 0;
  }
  void send() {
    b.u8(0);
    last = stack.back();
    stack.pop_back();
  }
  void lbegin(int16_t id, uint8_t et, uint32_t n) {
    field(id, 9);
    if (n < 15)
      b.u8(uint8_t((n << 4) | et));
    else {
      b.u8(uint8_t(0xF0 | et));
      b.varint(n);
    }
  }
  void ebegin() {  // struct element of a list
    stack.push_back(last);
    last = 0;
  }
  void eend() { send(); }
  void stop() { b.u8(0); }
};

struct Column {
  std::string name;
  int type = T_INT64;
  bool optional = false;
  bool is_string = false;  // UTF8 / STRING
  bool is_date = false;    // DATE on INT32
  // Plain-encoded non-null values (data page) unless dict is used.
  Buf values;
  // Optional dictionary: plain-encoded dictionary values + per-row index (non-null rows).
  bool use_dict = false;
  Buf dict_values;
  int dict_count = 0;
  std::vector<uint32_t> indices;  // one per non-null row
  // Definition levels (optional columns): one byte (0/1) per row.
  std::vector<uint8_t> defs;
  int64_t num_nulls = 0;
};

inline void put_i32(Buf& b, int32_t v) { b.bytes(&v, 4); }
inline void put_i64(Buf& b, int64_t v) { b.bytes(&v, 8); }
inline void put_str(Buf& b, const char* s, uint32_t n) {
  b.le32(n);
  b.bytes(s, n);
}

// RLE / bit-packed hybrid, bit-packed runs of at most 63 groups of 8 values.
inline void encode_bitpacked(Buf& out, const uint32_t* vals, size_t n, int bw) {
  size_t i = 0;
  while (i < n) {
    size_t remain = n - i;
    size_t groups = (remain + 7) / 8;
    if (groups > 63) groups = 63;
    out.u8(uint8_t((groups << 1) | 1));
    // pack groups*8 values with bit width bw, LSB first
    uint64_t acc = 0;
    int nb = 0;
    for (size_t k = 0; k < groups * 8; k++) {
      uint32_t v = (i + k < n) ? vals[i + k] : 0;
      acc |= (uint64_t)v << nb;
      nb += bw;
      while (nb >= 8) {
        out.u8(uint8_t(acc));
        acc >>= 8;
        nb -= 8;
      }
    }
    if (nb > 0) out.u8(uint8_t(acc));
    i += groups * 8;
  }
}

inline int bit_width(uint32_t maxv) {
  int w = 0;
  while (maxv) {
    w++;
    maxv >>= 1;
  }
  return w;
}

inline void page_header(Buf& out, int page_type, int32_t usize, int32_t csize, int32_t num_values, int encoding,
                        bool dict_page) {
  TC t(out);
  t.i32(1, page_type);
  t.i32(2, usize);
  t.i32(3, csize);
  if (dict_page) {
    t.sbegin(7);
    t.i32(1, num_values);
    t.i32(2, ENC_PLAIN);
    t.send();
  } else {
    t.sbegin(5);
    t.i32(1, num_values);
    t.i32(2, encoding);
    t.i32(3, ENC_RLE);
    t.i32(4, ENC_RLE);
    t.send();
  }
  t.stop();
}

struct ChunkMeta {
  int64_t data_page_offset = 0;
  int64_t dict_page_offset = -1;
  int64_t total_size = 0;
  int64_t num_values = 0;
};

// Bit-packed (width 1) runs for 0/1 definition levels.
inline void encode_defs(Buf& out, const std::vector<uint8_t>& defs) {
  size_t n = defs.size(), i = 0;
  out.d.reserve(out.d.size() + n / 8 + n / 504 + 8);
  while (i < n) {
    size_t groups = (n - i + 7) / 8;
    if (groups > 63) groups = 63;
    out.u8(uint8_t((groups << 1) | 1));
    for (size_t g = 0; g < groups; g++) {
      uint8_t byte = 0;
      for (int b = 0; b < 8; b++) {
        size_t k = i + g * 8 + b;
        if (k < n && defs[k]) byte |= uint8_t(1u << b);
      }
      out.u8(byte);
    }
    i += groups * 8;
  }
}

struct Out {
  FILE* f;
  int64_t pos = 0;
  bool ok = true;
  void put(const void* p, size_t n) {
    if (n && fwrite(p, 1, n, f) != n) ok = false;
    pos += (int64_t)n;
  }
  void put(const Buf& b) { put(b.d.data(), b.d.size()); }
};

// Writes a complete file. Returns false on I/O error.
inline bool write_file(const std::string& path, std::vector<Column>& cols, int64_t nrows) {
  Out o;
  o.f = fopen(path.c_str(), "wb");
  if (!o.f) return false;
  setvbuf(o.f, nullptr, _IOFBF, 1 << 20);
  const char magic[4] = {'P', 'A', 'R', '1'};
  o.put(magic, 4);
  std::vector<ChunkMeta> metas(cols.size());
  for (size_t ci = 0; ci < cols.size() && nrows > 0; ci++) {
    Column& c = cols[ci];
    ChunkMeta& m = metas[ci];
    m.num_values = nrows;
    int64_t start = o.pos;
    if (c.use_dict) {
      Buf h;
      page_header(h, 2, (int32_t)c.dict_values.size(), (int32_t)c.dict_values.size(), c.dict_count, ENC_PLAIN, true);
      m.dict_page_offset = o.pos;
      o.put(h);
      o.put(c.dict_values);
    }
    Buf pre;  // def levels and, for dictionary columns, the index stream
    if (c.optional) {
      Buf lv;
      encode_defs(lv, c.defs);
      pre.le32((uint32_t)lv.size());
      pre.bytes(lv.d.data(), lv.size());
    }
    int enc = ENC_PLAIN;
    size_t body = pre.size();
    if (c.use_dict) {
      enc = ENC_RLE_DICTIONARY;
      int bw = bit_width(c.dict_count > 0 ? (uint32_t)(c.dict_count - 1) : 0);
      if (bw < 1) bw = 1;
      pre.u8((uint8_t)bw);
      encode_bitpacked(pre, c.indices.data(), c.indices.size(), bw);
      body = pre.size();
    } else {
      body += c.values.size();
    }
    Buf h;
    page_header(h, 0, (int32_t)body, (int32_t)body, (int32_t)nrows, enc, false);
    m.data_page_offset = o.pos;
    o.put(h);
    o.put(pre);
    if (!c.use_dict) o.put(c.values);
    m.total_size = o.pos - start;
  }
  Buf fm;
  {
    TC t(fm);
    t.i32(1, 1);
    t.lbegin(2, 12, (uint32_t)cols.size() + 1);
    t.ebegin();
    t.str(4, "schema");
    t.i32(5, (int32_t)cols.size());
    t.eend();
    for (auto& c : cols) {
      t.ebegin();
      t.i32(1, c.type);
      t.i32(3, c.optional ? 1 : 0);
      t.str(4, c.name);
      if (c.is_string) {
        t.i32(6, 0);
        t.sbegin(10);
        t.sbegin(1);
        t.send();
        t.send();
      } else if (c.is_date) {
        t.i32(6, 6);
        t.sbegin(10);
        t.sbegin(6);
        t.send();
        t.send();
      }
      t.eend();
    }
    t.i64(3, nrows);
    int64_t total = 0;
    for (auto& m : metas) total += m.total_size;
    if (nrows > 0) {
      t.lbegin(4, 12, 1);
      t.ebegin();
      t.lbegin(1, 12, (uint32_t)cols.size());
      for (size_t ci = 0; ci < cols.size(); ci++) {
        Column& c = cols[ci];
        ChunkMeta& m = metas[ci];
        t.ebegin();
        t.i64(2, m.dict_page_offset >= 0 ? m.dict_page_offset : m.data_page_offset);
        t.sbegin(3);
        t.i32(1, c.type);
        if (c.use_dict) {
          t.lbegin(2, 5, 3);
          fm.zz32(ENC_PLAIN);
          fm.zz32(ENC_RLE);
          fm.zz32(ENC_RLE_DICTIONARY);
        } else {
          t.lbegin(2, 5, 2);
          fm.zz32(ENC_PLAIN);
          fm.zz32(ENC_RLE);
        }
        t.lbegin(3, 8, 1);
        fm.varint(c.name.size());
        fm.bytes(c.name.data(), c.name.size());
        t.i32(4, 0);
        t.i64(5, m.num_values);
        t.i64(6, m.total_size);
        t.i64(7, m.total_size);
        t.i64(9, m.data_page_offset);
        if (m.dict_page_offset >= 0) t.i64(11, m.dict_page_offset);
        t.send();
        t.eend();
      }
      t.i64(2, total);
      t.i64(3, nrows);
      t.eend();
    } else {
      t.lbegin(4, 12, 0);
    }
    t.str(6, "shopping-sessions native writer");
    t.stop();
  }
  o.put(fm);
  uint32_t fl = (uint32_t)fm.size();
  o.put(&fl, 4);
  o.put(magic, 4);
  bool ok = o.ok;
  if (fclose(o.f) != 0) ok = false;
  return ok;
}

}  // namespace pq
