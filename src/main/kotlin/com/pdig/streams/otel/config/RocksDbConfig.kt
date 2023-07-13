package com.pdig.streams.otel.config

import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.*

class RocksDbConfig : RocksDBConfigSetter {

    companion object {
        const val TOTAL_OFF_HEAP_SIZE_MB = "rocksdb.total_offheap_size_mb"
        const val TOTAL_MEMTABLE_MB = "rocksdb.total_memtable_mb"
    }

    private val byteFactor: Long = 1
    private val kbFactor = 1024 * byteFactor
    private val mbFactor = 1024 * kbFactor

    private var cache: Cache? = null
    private var writeBufferManager: WriteBufferManager? = null
    override fun setConfig(storeName: String, options: Options, configs: MutableMap<String, Any>) {
        if (cache == null || writeBufferManager == null) initCacheOnce(configs)
        val tableConfig = options.tableFormatConfig() as BlockBasedTableConfig
        tableConfig.setBlockCache(cache)
        options.setWriteBufferManager(writeBufferManager)
        options.setTableFormatConfig(tableConfig)
        options.close()
    }

    override fun close(storeName: String, options: Options) {
        cache?.close()
        writeBufferManager?.close()
    }

    @Synchronized
    fun initCacheOnce(configs: MutableMap<String, Any>) {
        if (cache != null && writeBufferManager != null) return
        val offHeapMb: Long = configs[TOTAL_OFF_HEAP_SIZE_MB] as Long
        val totalMemTableMemMb: Long = configs[TOTAL_MEMTABLE_MB] as Long
        if (cache == null) {
            cache = LRUCache(offHeapMb * mbFactor)
        }
        if (writeBufferManager == null) writeBufferManager = WriteBufferManager(totalMemTableMemMb * mbFactor, cache)
    }
}