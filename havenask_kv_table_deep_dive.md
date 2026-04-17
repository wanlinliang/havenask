# Havenask KV Table 存储引擎深度剖析

基于 `TestBuildIndexForSingleKV` 测试用例，从数据流、存储架构到并发安全的完整分析。

---

## 目录

- [1. 测试用例概览](#1-测试用例概览)
- [2. 写入数据流：String → Document → Segment → Indexer](#2-写入数据流string--document--segment--indexer)
  - [2.1 String → RawDocument](#21-string--rawdocument)
  - [2.2 RawDocument → KVDocumentBatch](#22-rawdocument--kvdocumentbatch)
  - [2.3 KVDocumentBatch → Segment (Indexer)](#23-kvdocumentbatch--segment-indexer)
- [3. 读取数据流：Key → Value](#3-读取数据流key--value)
- [4. 存储架构设计](#4-存储架构设计)
  - [4.1 Segment 生命周期](#41-segment-生命周期)
  - [4.2 Level 拓扑](#42-level-拓扑)
  - [4.3 Version 与 MVCC](#43-version-与-mvcc)
  - [4.4 Merge 策略](#44-merge-策略)
  - [4.5 Online vs Offline](#45-online-vs-offline)
- [5. 读写并发分离机制](#5-读写并发分离机制)
  - [5.1 宏观层：TabletData + shared_ptr 快照](#51-宏观层tabletdata--shared_ptr-快照)
  - [5.2 Segment 层：不可变快照](#52-segment-层不可变快照)
  - [5.3 HashTable 层：写序保证](#53-hashtable-层写序保证)
  - [5.4 ValueBuffer 层：Append-Only + Memory Barrier](#54-valuebuffer-层append-only--memory-barrier)
- [6. SpecialKeyBucket 并发安全深度分析](#6-specialkeybucket-并发安全深度分析)
  - [6.1 Bucket 内存布局](#61-bucket-内存布局)
  - [6.2 Value 类型与原子性分析](#62-value-类型与原子性分析)
  - [6.3 Torn Read 场景分析](#63-torn-read-场景分析)
  - [6.4 TimestampValue::SetValue() 防御机制](#64-timestampvaluesetvalue-防御机制)
- [7. GDB 调试指南](#7-gdb-调试指南)

---

## 1. 测试用例概览

**测试文件**: `aios/storage/indexlib/table/kv_table/test/KVTabletWriterTest.cpp` Line 220

**Schema**: `single_kv_schema.json`
- Key: `key` (LONG, number_hash)
- Value: `weights` (MultiFloat, fixed_multi_value_count=3)

**测试流程**:
```
PrepareSchema → PrepareTabletData → KVTabletWriter::Open
→ CreateDocumentBatch("cmd=add,key=1,weights=1.1 1.2 1.3;cmd=add,key=2,weights=2.1 2.2 2.3")
→ writer->Build(docBatch)
→ kvReader->Get(keytype_t(1)) → 验证 MultiFloat 值
```

---

## 2. 写入数据流：String → Document → Segment → Indexer

### 2.1 String → RawDocument

**入口**: `RawDocumentMaker::MakeBatch()`

```
"cmd=add,key=1,weights=1.1 1.2 1.3;cmd=add,key=2,weights=2.1 2.2 2.3"
```

1. 按 `;` 来分割为多个文档字符串
2. 每个文档字符串按 `,` 分割为字段
3. 每个字段按 `=` 分割为 key-value 对
4. 存入 `DefaultRawDocument` 的 `_fieldsIncrement` 向量（通过 `_hashMapIncrement` 做 fieldName → index 映射）

**产出**: `RawDocumentBatch`，包含多个 `DefaultRawDocument`（本质是 key-value map）

### 2.2 RawDocument → KVDocumentBatch

**调用链**:
```
KVDocumentBatchMaker::MakeBatchVec()
  → KVDocumentFactory::CreateDocumentParser()
    → KVDocumentParser::Parse()
      → 遍历 _indexDocParsers map
        → KVIndexDocumentParser::Parse()
          → ParseKey()   // HashKey
          → ParseValue() // 编码 value
```

**Key 编码** (`KVKeyExtractor`):
- `number_hash` 模式：直接取数值本身作为 key（hash("1") = 1）

**Value 编码** (`ValueConvertor`):
1. `Init()` 时为每个 value 字段创建 `MultiValueAttributeConvertor<float>`
2. 如果多字段，创建 `PackAttributeFormatter`
3. `ConvertMultiField()` 将空格替换为 `\x03`（MULTI_VALUE_DELIMITER）再编码

**fixed_multi_value_count=3 时的编码格式**:
```
[HashKey 8B][float1 4B][float2 4B][float3 4B] = 20 bytes total
```
无 count 头部（因为已知固定长度为 3）。

### 2.3 KVDocumentBatch → Segment (Indexer)

**调用链**:
```
KVTabletWriter::DoBuild()
  → CheckAndRewriteDocument()     // 处理 UPDATE_FIELD/DELETE，ADD 时跳过
  → CommonTabletWriter::DoBuild()
    → PlainMemSegment::Build()
      → KVMemIndexerBase::Build()
        → AddField()
          → _attrConvertor->Decode(value)  // 去掉 8B HashKey 前缀
          → _plainFormatEncoder->Encode()  // 可选
          → VarLenKVMemIndexer::Add(key, value, timestamp)
```

**VarLenKVMemIndexer::DoAdd()**:
```cpp
_valueWriter->Write(value, offset);  // 追加 value 到 ValueBuffer，返回 offset
_keyWriter->AddSimple(key, offset, ts);  // 插入 DenseHashTable
```

**底层存储结构**:
- **ValueBuffer** (`ExpandableValueAccessor`): 基于 slice 的追加式连续内存块
- **HashTable** (`DenseHashTable`): 线性探测哈希表，key → (offset, timestamp)

---

## 3. 读取数据流：Key → Value

```
kvReader->Get(keytype_t(1))
  → KVReaderImpl::InnerGet()
    → DoGet()
      → GetFromMemSegment()
        → VarLenKVMemoryReader::Get()
          → HashTable.FindForReadWrite(key)    // 查找 bucket，得到 offset+ts
          → ValueUnpacker.Unpack(ts, offset)   // 拆解 timestamp 和 offset
          → ValueAccessor.GetValue(offset)      // 从 ValueBuffer 读取 binary value
    → FieldValueExtractor::GetTypedValue("weights", multiFloat)
      → PackAttributeFormatter + AttributeReferenceTyped<T>
```

**GetKVReader(0)**: 0 是单索引情况的快捷方式，返回 `_kvReaders` map 中唯一的 reader。

---

## 4. 存储架构设计

### 4.1 Segment 生命周期

```
ST_BUILDING (内存，可变)
    → ST_DUMPING (正在 dump 到磁盘)
        → ST_BUILT (磁盘，不可变)
```

- **Building**: 唯一可写的 segment，内存中的 DenseHashTable + ValueBuffer
- **Dumping**: 冻结的内存 segment，正在序列化到磁盘
- **Built**: 磁盘上的不可变 segment，可被 merge

### 4.2 Level 拓扑

| 拓扑 | 层级 | 特点 |
|---|---|---|
| `topo_sequence` | L0 | segment 间可重叠，时间序排列 |
| `topo_hash_mod` | L1+ | merge 后，segment 间不重叠（按 hash 分片） |
| `topo_key_range` | - | 按 key 范围分片（用于其他表类型） |

### 4.3 Version 与 MVCC

- **Version**: segment 列表的不可变快照
- **TabletData**: 包含 Version + 对应的 segment 对象集合
- **MVCC**: 通过 `shared_ptr` 引用计数实现多版本
  - 新版本创建新 TabletData 对象
  - 旧 Reader 持有旧 TabletData 的 shared_ptr，不受影响
  - 最后一个 Reader 释放 shared_ptr 时，旧资源自然回收

### 4.4 Merge 策略

**目的**:
1. 减少读放大（减少需遍历的 segment 数量）
2. 回收空间（去重、TTL 淘汰）
3. 提高哈希表效率（CuckooHash 80% 填充率 vs DenseHash 50%）

### 4.5 Online vs Offline

| 特性 | Online | Offline |
|---|---|---|
| HashTable | DenseHash（50% occupancy，快） | CuckooHash（80% occupancy，紧凑） |
| Reader 创建 | 立即 | 延迟 open |
| Merge | 自动触发 | 手动触发 |

---

## 5. 读写并发分离机制

Havenask 的 KV table 实现了 **单 Writer + 多 Reader 的无锁并发**，依赖 x86 TSO 内存模型。

### 5.1 宏观层：TabletData + shared_ptr 快照

```
Writer                              Reader
  │                                   │
  ├─ Build() 持有 _dataMutex          ├─ 持有 shared_ptr<TabletData>
  │  (保证单 Writer)                   │  (老版本快照)
  │                                   │
  ├─ Dump/Reopen 时:                  │
  │  创建新 TabletData                │
  │  atomic swap 指针                 │
  │  (_tabletDataMutex < 1μs)        │
  │                                   │
  └─                                  └─ 自然释放旧 shared_ptr
```

### 5.2 Segment 层：不可变快照

- 新 TabletData 创建时，ST_BUILDING segment 被冻结变为 ST_DUMPING
- 新 building segment 被创建
- 旧 Reader 仍安全读取已冻结的 segment

### 5.3 HashTable 层：写序保证

`SpecialKeyBucket::Set()`:
```cpp
void Set(const _KT& key, const _VT& value) {
    mValue = value;  // 先写 value
    mKey = key;      // 后写 key
}
```

x86 TSO 保证：**store-store 不重排**。所以 Reader 看到 key 时，value 一定已经写好。

### 5.4 ValueBuffer 层：Append-Only + Memory Barrier

`ValueWriter::Append()`:
```cpp
memcpy(buffer + _usedBytes, data, len);  // 1. 先写数据
MEMORY_BARRIER();                         // 2. 编译器屏障
_usedBytes += len;                        // 3. 后更新长度
```

- `MEMORY_BARRIER()` 是编译器屏障（非 CPU 屏障），防止编译器重排
- x86 TSO 保证硬件层面不重排 store-store
- Reader 读到的 offset 一定指向已写完的数据

---

## 6. SpecialKeyBucket 并发安全深度分析

### 6.1 Bucket 内存布局

```cpp
// special_key_bucket.h
#pragma pack(push)
#pragma pack(4)
template <typename _KT, typename _VT, ...>
class SpecialKeyBucket {
    _KT mKey;                    // uint64_t, 8B
    union {
        _VT mValue;              // 大小取决于 _VT
        _KT mKeyInValue;        // uint64_t, 8B (用于 Delete 标记)
    };
};
#pragma pack(pop)
```

**关键**: union 大小 = `max(sizeof(_VT), sizeof(_KT))`，由于 `_KT = uint64_t = 8B`，union 至少 8B。

### 6.2 Value 类型与原子性分析

**类型定义**:
```cpp
typedef uint64_t offset_t;       // 8B
typedef uint32_t short_offset_t; // 4B
```

**四种 Value 类型在 Bucket 中的布局**:

#### OffsetValue\<short_offset_t\> — 4B ✅

```
bucket [0..7]   mKey (8B)
bucket [8..15]  union (8B, 因为 mKeyInValue=8B)
                  └─ mValue (OffsetValue, 仅用 [8..11] 的 4B)
sizeof(bucket) = 16B
```

SetValue: `mValue = value.Value()` → **1 条 4B MOV，天然原子**

#### TimestampValue\<short_offset_t\> — 8B ✅

TimestampValue 内部布局 (`#pragma pack(4)`):
```
+0: mTimestamp (uint32_t, 4B)
+4: mValue    (short_offset_t = uint32_t, 4B)
```

在 bucket 中:
```
bucket [0..7]   mKey
bucket [8..11]  mTimestamp (4B)
bucket [12..15] mValue    (4B)
sizeof(bucket) = 16B
```

SetValue 拆分为两次 4B 写入，各自不跨 8 字节边界，**每条都原子**。

#### OffsetValue\<offset_t\> — 8B ✅

```
bucket [0..7]   mKey
bucket [8..15]  union (8B)
                  └─ mValue (OffsetValue<offset_t>, 8B, [8..15])
sizeof(bucket) = 16B
```

SetValue: `mValue = value.Value()` → **8B 写入，起始于偏移 8（8 字节对齐），x86 保证原子**

#### TimestampValue\<offset_t\> — 12B ❌

```
bucket [0..7]   mKey
bucket [8..11]  mTimestamp (4B)
bucket [12..19] mValue (offset_t = 8B) ← 起始于偏移 12 !!!
sizeof(bucket) = 20B（pack(4) 下不补齐到 24）
```

`mValue` 写入 `[12..19]`，**跨越了偏移 16 的 8 字节边界**，x86 不保证原子性。

#### 汇总

| 类型 | Value 大小 | 关键写入 | Bucket 内偏移 | 跨 8B 边界？ | 原子？ |
|---|---|---|---|---|---|
| `OffsetValue<short_offset_t>` | 4B | `mValue` 4B | [8,12) | 否 | ✅ |
| `TimestampValue<short_offset_t>` | 8B | `mValue` 4B + `mTimestamp` 4B | [12,16) + [8,12) | 否 | ✅ |
| `OffsetValue<offset_t>` | 8B | `mValue` 8B | [8,16) 8B 对齐 | 否 | ✅ |
| `TimestampValue<offset_t>` | 12B | `mValue` 8B | **[12,20)** 4B 对齐 | **跨 16** | ❌ |

**注**: union 中 `mKeyInValue`（8B）保证了前三种情况 bucket 总大小为 16B（8 的倍数），数组排列时每个 bucket 的 `mKey` 始终 8 字节对齐。而 `TimestampValue<offset_t>` 使 union 增至 12B，bucket 总大小 20B，后续 bucket 的对齐也无法保证。

### 6.3 Torn Read 场景分析

#### 场景一：Key Torn Read（首次写入空 bucket）

```
Writer: Set(key, value) → 先写 value，后写 key
Reader: 遍历 bucket，比较 key
```

- bucket 跨缓存行的概率 ~6%（64B 缓存行 / bucket 大小）
- 如果 Reader 读到撕裂的 key，恰好等于搜索 key 的概率 ≈ $\frac{1}{2^{64}}$
- 即使匹配，value 已经写好了（写序保证），读到的仍是正确值
- **实际风险可忽略**

#### 场景二：Value Torn Read（更新已有 bucket）

当 value 更新时（同一个 key 写入新值），key 不变，Reader 一定能找到这个 bucket。

对于 `TimestampValue<offset_t>`，`mValue`（offset_t, 8B）的写入跨 8 字节边界，Reader 可能读到半新半旧的 offset → **指向非法内存位置 → 严重 bug**。

### 6.4 TimestampValue::SetValue() 防御机制

```cpp
// special_value.h
void SetValue(const TimestampValue<_RealVT>& value)
{
    if (!IsEmpty() && !IsDeleted()) {
        SetDelete(value);   // ① defend for value cross border of 8Bytes
    }
    mValue = value.Value();                      // ② 写 value（可能撕裂）
    volatile uint32_t tempTimestamp = value.Timestamp();
    mTimestamp = tempTimestamp;                   // ③ 恢复正常 timestamp
}

void SetDelete(const TimestampValue<_RealVT>& value)
{
    volatile uint32_t tempTimestamp = value.Timestamp() | DeleteMask;
    mTimestamp = tempTimestamp;  // 设置 DeleteMask
}
```

**协议：Delete → Write → Restore**

```
时间线:
──────────────────────────────────────────────────
① SetDelete(): mTimestamp |= DeleteMask
   → bucket 状态变为 DELETED
   → 此时 Reader 看到 IsDeleted()=true，跳过此 bucket

② mValue = value.Value()
   → 8B 写入可能撕裂
   → 但 bucket 处于 DELETED 状态，Reader 不会读 value

③ mTimestamp = tempTimestamp (不含 DeleteMask)
   → bucket 恢复 VALID 状态
   → mTimestamp 写入是 4B，天然原子
   → x86 TSO 保证: Reader 看到新 mTimestamp 时，② 的写入一定完成
──────────────────────────────────────────────────
```

**x86 TSO 保证**: store ② happens-before store ③，Reader 看到 ③（VALID 状态）时 ② 一定完成。因此 Reader 永远不会读到撕裂的 offset。

**为什么有 `if (!IsEmpty() && !IsDeleted())` 守卫？**
- Empty bucket：首次写入，不存在旧值被读的问题（Reader 还没看到 key）
- Deleted bucket：已经是删除状态，Reader 已经不会读 value
- 只有 Valid → Valid 的更新才需要防御

---

## 7. GDB 调试指南

### 编译 debug 版本

```bash
cd /home/liangwanlin/havenask
bazel build -c dbg //aios/storage/indexlib/table/kv_table/test:KVTabletWriterTest
```

### 启动 GDB

```bash
gdb ./bazel-bin/aios/storage/indexlib/table/kv_table/test/KVTabletWriterTest
```

### 优化启动速度

```
(gdb) set auto-solib-add off      # 不自动加载 .so 符号
(gdb) set print thread-events off  # 不打印线程创建/销毁事件
```

### 关键断点

```
# 写入流程
b RawDocumentMaker::MakeBatch
b KVDocumentParser::Parse
b KVIndexDocumentParser::ParseKey
b KVIndexDocumentParser::ParseValue
b ValueConvertor::ConvertValue
b KVTabletWriter::DoBuild
b KVMemIndexerBase::Build
b KVMemIndexerBase::AddField
b VarLenKVMemIndexer::DoAdd

# 读取流程
b KVReaderImpl::InnerGet
b VarLenKVMemoryReader::Get
```

### 运行指定测试

```
(gdb) run --gtest_filter=KVTabletWriterTest.TestBuildIndexForSingleKV
```

### 检查 RawDocument 内部数据

GDB 中无法直接调用含 StringView 参数的方法，需直接访问内部字段：

```
(gdb) p ((DefaultRawDocument*)rawDoc)->_fieldsIncrement
(gdb) p ((DefaultRawDocument*)rawDoc)->_hashMapIncrement
```

---

## 附录：关键源文件索引

| 文件 | 说明 |
|---|---|
| `table/kv_table/test/KVTabletWriterTest.cpp` | 测试入口 |
| `table/kv_table/test/single_kv_schema.json` | 测试 schema |
| `document/RawDocumentMaker.cpp` | String → RawDocument |
| `table/kv_table/KVDocumentBatchMaker.cpp` | RawDoc → KVDocumentBatch |
| `index/kv/KVDocumentParser.cpp` | 文档解析入口 |
| `index/kv/KVIndexDocumentParser.cpp` | 单索引文档解析 |
| `index/kv/ValueConvertor.cpp` | Value 字段编码 |
| `index/attribute/MultiValueAttributeConvertor.h` | 多值编码 |
| `table/kv_table/KVTabletWriter.cpp` | Writer 主逻辑 |
| `index/kv/KVMemIndexerBase.cpp` | 内存索引构建 |
| `index/kv/VarLenKVMemIndexer.cpp` | 变长 KV 索引 |
| `index/common/hash_table/DenseHashTable.h` | 哈希表实现 |
| `index/common/hash_table/SpecialKeyBucket.h` | Bucket 实现 |
| `index/common/hash_table/SpecialValue.h` | Value 类型 + 防御机制 |
| `index/kv/VarLenKVMemoryReader.h` | 内存 segment reader |
| `index/kv/KVReaderImpl.h` | Reader 主逻辑 |
| `framework/Tablet.cpp` | TabletData 管理 |
