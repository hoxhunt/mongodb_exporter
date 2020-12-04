package mongod

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/mongodb_exporter/collector/common"
	"github.com/percona/mongodb_exporter/shared"
)

var (
	collectionSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "size",
		Help:      "The total size in memory of all records in a collection",
	}, []string{"db", "coll"})
	collectionObjectCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "count",
		Help:      "The number of objects or documents in this collection",
	}, []string{"db", "coll"})
	collectionAvgObjSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "avgobjsize",
		Help:      "The average size of an object in the collection (plus any padding)",
	}, []string{"db", "coll"})
	collectionStorageSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "storage_size",
		Help:      "The total amount of storage allocated to this collection for document storage",
	}, []string{"db", "coll"})
	collectionIndexes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes",
		Help:      "The number of indexes on the collection",
	}, []string{"db", "coll"})
	collectionIndexesSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "indexes_size",
		Help:      "The total size of all indexes",
	}, []string{"db", "coll"})
	collectionIndexSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "db_coll",
		Name:      "index_size",
		Help:      "The individual index size",
	}, []string{"db", "coll", "index"})
)

var (
	collWTBlockManagerBlocksTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_blockmanager",
		Name:      "blocks_total",
		Help:      "The total number of blocks allocated by the WiredTiger BlockManager",
	}, []string{"db", "coll", "type"})
)

var (
	collWTCachePages = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_cache",
		Name:      "pages",
		Help:      "The current number of pages in the WiredTiger Cache",
	}, []string{"db", "coll", "type"})
	collWTCachePagesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_cache",
		Name:      "pages_total",
		Help:      "The total number of pages read into/from the WiredTiger Cache",
	}, []string{"db", "coll", "type"})
	collWTCacheBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_cache",
		Name:      "bytes",
		Help:      "The current size of data in the WiredTiger Cache in bytes",
	}, []string{"db", "coll", "type"})
	collWTCacheBytesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_cache",
		Name:      "bytes_total",
		Help:      "The total number of bytes read into/from the WiredTiger Cache",
	}, []string{"db", "coll", "type"})
	collWTCacheEvictedTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_cache",
		Name:      "evicted_total",
		Help:      "The total number of pages evicted from the WiredTiger Cache",
	}, []string{"db", "coll", "type"})
)

var (
	collWTTransactionsUpdateConflicts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_transactions",
		Name:      "update_conflicts",
		Help:      "The number of conflicts updating transactions",
	}, []string{"db", "coll"})
)

var (
	collWTOpenCursors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "collection_wiredtiger_session",
		Name:      "open_cursors_total",
		Help:      "The total number of cursors opened in WiredTiger",
	}, []string{"db", "coll"})
)

// CollWTBlockManagerStats defines the blockmanager stats
type CollWTBlockManagerStats struct {
	BlocksFreed     float64 `bson:"blocks freed"`
	BlocksAllocated float64 `bson:"blocks allocated"`
}

// Export exports the collection block manager stats to prometheus
func (stats *CollWTBlockManagerStats) Export(ch chan<- prometheus.Metric, db, collection string) {
	collWTBlockManagerBlocksTotal.With(stats.labels(db, collection, "freed")).Set(stats.BlocksFreed)
	collWTBlockManagerBlocksTotal.With(stats.labels(db, collection, "allocated")).Set(stats.BlocksAllocated)
}

// Describe describes collection block manager stats for prometheus
func (stats *CollWTBlockManagerStats) Describe(ch chan<- *prometheus.Desc) {
	collWTBlockManagerBlocksTotal.Describe(ch)
}

func (stats *CollWTBlockManagerStats) labels(db, coll, kind string) prometheus.Labels {
	return prometheus.Labels{
		"db":   db,
		"coll": coll,
		"type": kind,
	}
}

// CollWTCacheStats defines the cache stats
type CollWTCacheStats struct {
	BytesTotal        float64 `bson:"bytes currently in the cache"`
	BytesDirty        float64 `bson:"tracked dirty bytes in the cache"`
	BytesReadInto     float64 `bson:"bytes read into cache"`
	BytesWrittenFrom  float64 `bson:"bytes written from cache"`
	EvictedUnmodified float64 `bson:"unmodified pages evicted"`
	EvictedModified   float64 `bson:"modified pages evicted"`
	PagesReadInto     float64 `bson:"pages read into cache"`
	PagesWrittenFrom  float64 `bson:"pages written from cache"`
}

// Export exports the collection cache stats to prometheus
func (stats *CollWTCacheStats) Export(ch chan<- prometheus.Metric, db, collection string) {
	collWTCachePagesTotal.With(stats.labels(db, collection, "read")).Set(stats.PagesReadInto)
	collWTCachePagesTotal.With(stats.labels(db, collection, "written")).Set(stats.PagesWrittenFrom)
	collWTCacheBytesTotal.With(stats.labels(db, collection, "read")).Set(stats.BytesReadInto)
	collWTCacheBytesTotal.With(stats.labels(db, collection, "written")).Set(stats.BytesWrittenFrom)
	collWTCacheEvictedTotal.With(stats.labels(db, collection, "modified")).Set(stats.EvictedModified)
	collWTCacheEvictedTotal.With(stats.labels(db, collection, "unmodified")).Set(stats.EvictedUnmodified)
	collWTCacheBytes.With(stats.labels(db, collection, "total")).Set(stats.BytesTotal)
	collWTCacheBytes.With(stats.labels(db, collection, "dirty")).Set(stats.BytesDirty)
}

// Describe describes the stats to prometheus
func (stats *CollWTCacheStats) Describe(ch chan<- *prometheus.Desc) {
	collWTCachePagesTotal.Describe(ch)
	collWTCacheEvictedTotal.Describe(ch)
	collWTCachePages.Describe(ch)
	collWTCacheBytes.Describe(ch)
}

func (stats *CollWTCacheStats) labels(db, coll, kind string) prometheus.Labels {
	return prometheus.Labels{
		"db":   db,
		"coll": coll,
		"type": kind,
	}
}

// CollWTSessionStats defines the session stats
type CollWTSessionStats struct {
	Cursors float64 `bson:"open cursor count"`
}

// Export exports the collection session stats to prometheus
func (stats *CollWTSessionStats) Export(ch chan<- prometheus.Metric, db, collection string) {
	collWTOpenCursors.With(prometheus.Labels{
		"db":   db,
		"coll": collection,
	}).Set(stats.Cursors)
}

// Describe describes collection session stats for prometheus
func (stats *CollWTSessionStats) Describe(ch chan<- *prometheus.Desc) {
	collWTOpenCursors.Describe(ch)
}

// CollWTTransactionStats defines the transaction stats
type CollWTTransactionStats struct {
	UpdateConflicts float64 `bson:"update conflicts"`
}

// Export exports the collection transaction stats to prometheus
func (stats *CollWTTransactionStats) Export(ch chan<- prometheus.Metric, db, collection string) {
	collWTTransactionsUpdateConflicts.With(prometheus.Labels{
		"db":   db,
		"coll": collection,
	}).Set(stats.UpdateConflicts)
}

// Describe describes collection transaction stats for prometheus
func (stats *CollWTTransactionStats) Describe(ch chan<- *prometheus.Desc) {
	collWTTransactionsUpdateConflicts.Describe(ch)
}

// CollWiredTigerStats defines the collection WiredTiger stats
type CollWiredTigerStats struct {
	BlockManager *CollWTBlockManagerStats `bson:"block-manager"`
	Cache        *CollWTCacheStats        `bson:"cache"`
	Session      *CollWTSessionStats      `bson:"session"`
	Transaction  *CollWTTransactionStats  `bson:"transaction"`
}

// Describe describes collection wired tiger stats for prometheus
func (stats *CollWiredTigerStats) Describe(ch chan<- *prometheus.Desc) {
	if stats.BlockManager != nil {
		stats.BlockManager.Describe(ch)
	}

	if stats.Cache != nil {
		stats.Cache.Describe(ch)
	}
	if stats.Transaction != nil {
		stats.Transaction.Describe(ch)
	}
	if stats.Session != nil {
		stats.Session.Describe(ch)
	}
}

// Export exports the collection wired tiger stats to prometheus
func (stats *CollWiredTigerStats) Export(ch chan<- prometheus.Metric, db, collection string) {
	if stats.BlockManager != nil {
		stats.BlockManager.Export(ch, db, collection)
	}

	if stats.Cache != nil {
		stats.Cache.Export(ch, db, collection)
	}

	if stats.Transaction != nil {
		stats.Transaction.Export(ch, db, collection)
	}

	if stats.Session != nil {
		stats.Session.Export(ch, db, collection)
	}

	collWTBlockManagerBlocksTotal.Collect(ch)
	collWTCachePagesTotal.Collect(ch)
	collWTCacheBytesTotal.Collect(ch)
	collWTCacheEvictedTotal.Collect(ch)
	collWTCachePages.Collect(ch)
	collWTCacheBytes.Collect(ch)
	collWTTransactionsUpdateConflicts.Collect(ch)
	collWTOpenCursors.Collect(ch)

	collWTBlockManagerBlocksTotal.Reset()
	collWTCachePagesTotal.Reset()
	collWTCacheBytesTotal.Reset()
	collWTCacheEvictedTotal.Reset()
	collWTCachePages.Reset()
	collWTCacheBytes.Reset()
	collWTTransactionsUpdateConflicts.Reset()
	collWTOpenCursors.Reset()
}

// CollectionStatList contains stats from all collections.
type CollectionStatList struct {
	Members []CollectionStatus
}

// CollectionStatus represents stats about a collection in database (mongod and raw from mongos).
type CollectionStatus struct {
	Database    string
	Name        string
	Size        int                  `bson:"size,omitempty"`
	Count       int                  `bson:"count,omitempty"`
	AvgObjSize  int                  `bson:"avgObjSize,omitempty"`
	StorageSize int                  `bson:"storageSize,omitempty"`
	IndexesSize int                  `bson:"totalIndexSize,omitempty"`
	IndexSizes  map[string]float64   `bson:"indexSizes,omitempty"`
	WiredTiger  *CollWiredTigerStats `bson:"wiredTiger"`
}

// Export exports database stats to prometheus.
func (collStatList *CollectionStatList) Export(ch chan<- prometheus.Metric) {
	// reset previously collected values
	collectionSize.Reset()
	collectionObjectCount.Reset()
	collectionAvgObjSize.Reset()
	collectionStorageSize.Reset()
	collectionIndexes.Reset()
	collectionIndexesSize.Reset()
	collectionIndexSize.Reset()
	for _, member := range collStatList.Members {
		ls := prometheus.Labels{
			"db":   member.Database,
			"coll": member.Name,
		}
		collectionSize.With(ls).Set(float64(member.Size))
		collectionObjectCount.With(ls).Set(float64(member.Count))
		collectionAvgObjSize.With(ls).Set(float64(member.AvgObjSize))
		collectionStorageSize.With(ls).Set(float64(member.StorageSize))
		collectionIndexes.With(ls).Set(float64(len(member.IndexSizes)))
		collectionIndexesSize.With(ls).Set(float64(member.IndexesSize))
		for indexName, size := range member.IndexSizes {
			ls = prometheus.Labels{
				"db":    member.Database,
				"coll":  member.Name,
				"index": indexName,
			}
			collectionIndexSize.With(ls).Set(size)
		}
		if member.WiredTiger != nil {
			member.WiredTiger.Export(ch, member.Database, member.Name)
		}
	}
	collectionSize.Collect(ch)
	collectionObjectCount.Collect(ch)
	collectionAvgObjSize.Collect(ch)
	collectionStorageSize.Collect(ch)
	collectionIndexes.Collect(ch)
	collectionIndexesSize.Collect(ch)
	collectionIndexSize.Collect(ch)
}

// Describe describes database stats for prometheus.
func (collStatList *CollectionStatList) Describe(ch chan<- *prometheus.Desc) {
	collectionSize.Describe(ch)
	collectionObjectCount.Describe(ch)
	collectionAvgObjSize.Describe(ch)
	collectionStorageSize.Describe(ch)
	collectionIndexes.Describe(ch)
	collectionIndexesSize.Describe(ch)

	if len(collStatList.Members) > 0 {
		member := collStatList.Members[0]
		if member.WiredTiger != nil {
			member.WiredTiger.Describe(ch)
		}
	}
}

var logSuppressCS = shared.NewSyncStringSet()

const keyCS = ""

// GetCollectionStatList returns stats for all non-system collections.
func GetCollectionStatList(client *mongo.Client) *CollectionStatList {
	collectionStatList := &CollectionStatList{}
	dbNames, err := client.ListDatabaseNames(context.TODO(), bson.M{})
	if err != nil {
		if !logSuppressCS.Contains(keyCS) {
			log.Warnf("%s. Collection stats will not be collected. This log message will be suppressed from now.", err)
			logSuppressCS.Add(keyCS)
		}
		return nil
	}

	logSuppressCS.Delete(keyCS)
	for _, dbName := range dbNames {
		if common.IsSystemDB(dbName) {
			continue
		}

		collNames, err := client.Database(dbName).ListCollectionNames(context.TODO(), bson.M{})
		if err != nil {
			if !logSuppressCS.Contains(dbName) {
				log.Warnf("%s. Collection stats will not be collected for this db. This log message will be suppressed from now.", err)
				logSuppressCS.Add(dbName)
			}
			continue
		}

		logSuppressCS.Delete(dbName)
		for _, collName := range collNames {
			if common.IsSystemCollection(collName) {
				continue
			}

			fullCollName := common.CollFullName(dbName, collName)
			collStatus := CollectionStatus{}
			err = client.Database(dbName).RunCommand(context.TODO(), bson.D{{"collStats", collName}, {"scale", 1}}).Decode(&collStatus)
			if err != nil {
				if !logSuppressCS.Contains(fullCollName) {
					log.Warnf("%s. Collection stats will not be collected for this collection. This log message will be suppressed from now.", err)
					logSuppressCS.Add(fullCollName)
				}
				continue
			}

			logSuppressCS.Delete(fullCollName)
			collStatus.Database = dbName
			collStatus.Name = collName
			collectionStatList.Members = append(collectionStatList.Members, collStatus)
		}
	}

	return collectionStatList
}
