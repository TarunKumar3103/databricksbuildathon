// Minimal Parquet writer: one row group per file, one data page (v1) per column, PLAIN values, optional
// dictionary pages, no compression. Enough for flat tables of INT32 / INT64 / DATE / STRING columns,
// required or optional.
//
// A column's values are handed over as pieces of memory in their on-disk form (PLAIN encoding is just the
// values, little-endian; strings as a 4-byte length and the bytes). The pieces are written as they are with
// writev, so the caller can build them wherever it likes and nothing is copied in user space.
#pragma once
#include <fcntl.h>
#include <immintrin.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace pq {

enum PhysType { T_INT32 = 1, T_INT64 = 2, T_BYTE_ARRAY = 6 };
enum { ENC_PLAIN = 0, ENC_RLE = 3, ENC_RLE_DICTIONARY = 8 };

// Growable byte buffer. The vector is only storage (its size is the capacity); `n` bytes are in use.
struct Buf {
  std::vector<uint8_t> d;
  size_t n = 0;
  void need(size_t k) {
    if (__builtin_expect(n + k > d.size(), 0)) d.resize((n + k) * 2 + 64);
  }
  void u8(uint8_t x) {
    need(1);
    d[n++] = x;
  }
  void bytes(const void* p, size_t k) {
    need(k);
    memcpy(d.data() + n, p, k);
    n += k;
  }
  void varint(uint64_t x) {
    while (x >= 0x80) {
      u8(uint8_t(x) | 0x80);
      x >>= 7;
    }
    u8(uint8_t(x));
  }
  void zz32(int32_t x) { varint((uint32_t)(((uint32_t)x << 1) ^ (uint32_t)(x >> 31))); }
  void zz64(int64_t x) { varint((uint64_t)(((uint64_t)x << 1) ^ (uint64_t)(x >> 63))); }
  void le32(uint32_t x) { bytes(&x, 4); }
  size_t size() const { return n; }
  const uint8_t* data() const { return d.data(); }
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

struct Piece {
  const void* p;
  size_t n;
};

struct Column {
  std::string name;
  int type = T_INT64;
  bool optional = false;
  bool is_string = false;  // UTF8 / STRING
  bool is_date = false;    // DATE on INT32
  // The data page is `pre` followed by `body`. pre: the definition levels of an optional column and, for a
  // dictionary column, the bit width and the indices. body: the PLAIN values of the non-null rows.
  Buf pre;
  std::vector<Piece> body;
  bool use_dict = false;
  Buf dict_values;               // PLAIN dictionary values ...
  std::vector<Piece> dict_body;  // ... followed by these
  int dict_count = 0;
};

inline void put_str(Buf& b, const char* s, uint32_t n) {
  b.le32(n);
  b.bytes(s, n);
}

inline int bit_width(uint32_t maxv) {
  int w = 0;
  while (maxv) {
    w++;
    maxv >>= 1;
  }
  return w;
}

// RLE / bit-packed hybrid with bit-packed runs only (at most 63 groups of 8 values each).
// Width 1, from one byte (0 or 1) per value: definition levels.
inline void encode_levels(Buf& out, const uint8_t* v01, size_t n) {
  out.need(n / 8 + n / 504 + 16);
  size_t i = 0;
  while (i < n) {
    size_t groups = (n - i + 7) / 8;
    if (groups > 63) groups = 63;
    out.u8(uint8_t((groups << 1) | 1));
    for (size_t g = 0; g < groups; g++, i += 8) {
      uint64_t w = 0;
      memcpy(&w, v01 + i, i + 8 <= n ? 8 : n - i);
      out.u8((uint8_t)_pext_u64(w, 0x0101010101010101ULL));
    }
  }
}
// Width bw (1..8), from one byte per value: dictionary indices.
// Dictionary indices of at most 8 bits each, one per byte at `vals`, as a single bit-packed run. Eight values
// are packed per step. At least 8 * ceil(n / 8) bytes must be readable at `vals` (the padding must be 0).
inline void encode_packed8(Buf& out, const uint8_t* vals, size_t n, int bw) {
  const size_t groups = (n + 7) / 8;
  out.varint((uint64_t(groups) << 1) | 1);
  out.need(groups * (size_t)bw + 16);
  const uint64_t mask = 0x0101010101010101ULL * ((1u << bw) - 1);
  uint8_t* o = out.d.data() + out.n;
  for (size_t g = 0; g < groups; g++) {
    uint64_t x;
    memcpy(&x, vals + 8 * g, 8);
    x = _pext_u64(x, mask);
    memcpy(o, &x, 8);
    o += bw;
  }
  out.n += groups * (size_t)bw;
}

// The same for indices of at most 16 bits each, one per 16-bit word; 8 * ceil(n / 8) words must be readable.
inline void encode_packed16(Buf& out, const uint16_t* vals, size_t n, int bw) {
  const size_t groups = (n + 7) / 8;
  out.varint((uint64_t(groups) << 1) | 1);
  out.need(groups * (size_t)bw + 32);
  const uint64_t mask = 0x0001000100010001ULL * ((1u << bw) - 1);
  const int s = 4 * bw;  // bits of half a group (4 .. 64)
  uint8_t* o = out.d.data() + out.n;
  for (size_t g = 0; g < groups; g++) {
    uint64_t a, b;
    memcpy(&a, vals + 8 * g, 8);
    memcpy(&b, vals + 8 * g + 4, 8);
    a = _pext_u64(a, mask);
    b = _pext_u64(b, mask);
    const uint64_t w0 = s < 64 ? a | (b << s) : a;
    const uint64_t w1 = s < 64 ? b >> (64 - s) : b;
    memcpy(o, &w0, 8);
    memcpy(o + 8, &w1, 8);
    o += bw;
  }
  out.n += groups * (size_t)bw;
}

// Dictionary indices first, first + 1, ... each repeated counts[i] times (1 .. 255), as one RLE run per index.
// bw is the bit width of the largest index.
inline void encode_runs(Buf& out, const uint8_t* counts, size_t n, uint32_t first, int bw) {
  const size_t vb = (size_t)(bw + 7) / 8;
  out.need(n * (vb + 2) + 16);
  uint8_t* o = out.d.data() + out.n;
  for (size_t i = 0; i < n; i++) {
    const uint32_t h = (uint32_t)counts[i] << 1, v = first + (uint32_t)i;
    const uint32_t two = h >= 0x80;  // the run header is a varint of one or two bytes
    o[0] = (uint8_t)(h | (two << 7));
    o[1] = (uint8_t)(h >> 7);
    o += 1 + two;
    memcpy(o, &v, 4);  // (room for the whole word was reserved)
    o += vb;
  }
  out.n = (size_t)(o - out.d.data());
}

inline void encode_indices(Buf& out, const uint8_t* vals, size_t n, int bw) {
  out.need((n * bw + 7) / 8 + n / 504 + 16);
  size_t i = 0;
  while (i < n) {
    size_t groups = (n - i + 7) / 8;
    if (groups > 63) groups = 63;
    out.u8(uint8_t((groups << 1) | 1));
    uint64_t acc = 0;
    int nb = 0;
    for (size_t k = 0; k < groups * 8; k++) {
      const uint32_t v = (i + k < n) ? vals[i + k] : 0;
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

// Writes a complete file. Returns false on I/O error.
inline bool write_file(const std::string& path, std::vector<Column>& cols, int64_t nrows) {
  static const char magic[4] = {'P', 'A', 'R', '1'};
  std::vector<iovec> iov;
  int64_t pos = 0;
  auto add = [&](const void* p, size_t n) {
    if (n) iov.push_back(iovec{(void*)p, n});
    pos += (int64_t)n;
  };
  std::vector<Buf> heads(cols.size() * 2);  // page headers; never resized, so their addresses hold
  size_t nheads = 0;
  std::vector<ChunkMeta> metas(cols.size());
  add(magic, 4);
  for (size_t ci = 0; ci < cols.size() && nrows > 0; ci++) {
    Column& c = cols[ci];
    ChunkMeta& m = metas[ci];
    m.num_values = nrows;
    const int64_t start = pos;
    if (c.use_dict) {
      Buf& h = heads[nheads++];
      size_t dsz = c.dict_values.size();
      for (const Piece& pc : c.dict_body) dsz += pc.n;
      page_header(h, 2, (int32_t)dsz, (int32_t)dsz, c.dict_count, ENC_PLAIN, true);
      m.dict_page_offset = pos;
      add(h.data(), h.size());
      add(c.dict_values.data(), c.dict_values.size());
      for (const Piece& pc : c.dict_body) add(pc.p, pc.n);
    }
    size_t body = c.pre.size();
    for (const Piece& pc : c.body) body += pc.n;
    Buf& h = heads[nheads++];
    page_header(h, 0, (int32_t)body, (int32_t)body, (int32_t)nrows, c.use_dict ? ENC_RLE_DICTIONARY : ENC_PLAIN, false);
    m.data_page_offset = pos;
    add(h.data(), h.size());
    add(c.pre.data(), c.pre.size());
    for (const Piece& pc : c.body) add(pc.p, pc.n);
    m.total_size = pos - start;
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
  const uint32_t fl = (uint32_t)fm.size();
  add(fm.data(), fm.size());
  add(&fl, 4);
  add(magic, 4);

  const int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) return false;
  bool ok = true;
  size_t i = 0;
  while (i < iov.size()) {
    const int cnt = (int)(iov.size() - i < 512 ? iov.size() - i : 512);
    ssize_t w = writev(fd, &iov[i], cnt);
    if (w < 0) {
      if (errno == EINTR) continue;
      ok = false;
      break;
    }
    while (w > 0 && i < iov.size()) {  // a short write ends inside a piece
      if ((size_t)w >= iov[i].iov_len) {
        w -= (ssize_t)iov[i].iov_len;
        i++;
      } else {
        iov[i].iov_base = (char*)iov[i].iov_base + w;
        iov[i].iov_len -= (size_t)w;
        w = 0;
      }
    }
  }
  if (close(fd) != 0) ok = false;
  return ok;
}

}  // namespace pq
