package base // import "github.com/cockroachdb/pebble/internal/base"

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/redact"
)

// SeqNum is a sequence number defining precedence among identical keys. A key
// with a higher sequence number takes precedence over a key with an equal user
// key of a lower sequence number. Sequence numbers are stored durably within
// the internal key "trailer" as a 7-byte (uint56) uint, and the maximum
// sequence number is 2^56-1. As keys are committed to the database, they're
// assigned increasing sequence numbers. Readers use sequence numbers to read a
// consistent database state, ignoring keys with sequence numbers larger than
// the readers' "visible sequence number."
//
// The database maintains an invariant that no two point keys with equal user
// keys may have equal sequence numbers. Keys with differing user keys may have
// equal sequence numbers. A point key and a range deletion or range key that
// include that point key can have equal sequence numbers - in that case, the
// range key does not apply to the point key. A key's sequence number may be
// changed to zero during compactions when it can be proven that no identical
// keys with lower sequence numbers exist.
type SeqNum uint64

const (
	// SeqNumZero is the zero sequence number, set by compactions if they can
	// guarantee there are no keys underneath an internal key.
	SeqNumZero SeqNum = 0
	// SeqNumStart is the first sequence number assigned to a key. Sequence
	// numbers 1-9 are reserved for potential future use.
	SeqNumStart SeqNum = 10
	// SeqNumMax is the largest valid sequence number.
	SeqNumMax SeqNum = 1<<56 - 1
	// SeqNumBatchBit is set on batch sequence numbers which prevents those
	// entries from being excluded from iteration.
	SeqNumBatchBit SeqNum = 1 << 55
)

func (s SeqNum) String() string {
	if s == SeqNumMax {
		return "inf"
	}
	var batch string
	if s&SeqNumBatchBit != 0 {
		batch = "b"
		s &^= SeqNumBatchBit
	}
	return fmt.Sprintf("%s%d", batch, s)
}

// SafeFormat implements redact.SafeFormatter.
func (s SeqNum) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(redact.SafeString(s.String()))
}

// InternalKeyKind enumerates the kind of key: a deletion tombstone, a set
// value, a merged value, etc.
type InternalKeyKind uint8

var internalKeyKindNames = []string{
	InternalKeyKindDelete:         "DEL",
	InternalKeyKindSet:            "SET",
	InternalKeyKindMerge:          "MERGE",
	InternalKeyKindLogData:        "LOGDATA",
	InternalKeyKindSingleDelete:   "SINGLEDEL",
	InternalKeyKindRangeDelete:    "RANGEDEL",
	InternalKeyKindSeparator:      "SEPARATOR",
	InternalKeyKindSetWithDelete:  "SETWITHDEL",
	InternalKeyKindRangeKeySet:    "RANGEKEYSET",
	InternalKeyKindRangeKeyUnset:  "RANGEKEYUNSET",
	InternalKeyKindRangeKeyDelete: "RANGEKEYDEL",
	InternalKeyKindIngestSST:      "INGESTSST",
	InternalKeyKindDeleteSized:    "DELSIZED",
	InternalKeyKindExcise:         "EXCISE",
	InternalKeyKindInvalid:        "INVALID",
}

func (k InternalKeyKind) String() string {
	if int(k) < len(internalKeyKindNames) {
		return internalKeyKindNames[k]
	}
	return fmt.Sprintf("UNKNOWN:%d", k)
}

// InternalKeyTrailer encodes a SeqNum and an InternalKeyKind.
type InternalKeyTrailer uint64

// MakeTrailer constructs an internal key trailer from the specified sequence
// number and kind.
func MakeTrailer(seqNum SeqNum, kind InternalKeyKind) InternalKeyTrailer {
	return (InternalKeyTrailer(seqNum) << 8) | InternalKeyTrailer(kind)
}

// String imlements the fmt.Stringer interface.
func (t InternalKeyTrailer) String() string {
	return fmt.Sprintf("%s,%s", SeqNum(t>>8), InternalKeyKind(t&0xff))
}

// SeqNum returns the sequence number component of the trailer.
func (t InternalKeyTrailer) SeqNum() SeqNum {
	return SeqNum(t >> 8)
}

// Kind returns the key kind component of the trailer.
func (t InternalKeyTrailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & 0xff)
}

// IsExclusiveSentinel returns true if the trailer is a sentinel for an
// exclusive boundary.
func (t InternalKeyTrailer) IsExclusiveSentinel() bool {
	return t.SeqNum() == SeqNumMax
}

// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// It consists of the user key (as given by the code that uses package pebble)
// followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type InternalKey struct {
	UserKey []byte
	Trailer InternalKeyTrailer
}

// Kind returns the kind component of the key.
func (k InternalKey) Kind() InternalKeyKind {
	return k.Trailer.Kind()
}

// InternalTrailerLen is the number of bytes used to encode InternalKey.Trailer.
const InternalTrailerLen = 8

const (
	InternalKeyKindDelete  InternalKeyKind = 0
	InternalKeyKindSet     InternalKeyKind = 1
	InternalKeyKindMerge   InternalKeyKind = 2
	InternalKeyKindLogData InternalKeyKind = 3
	//InternalKeyKindColumnFamilyDeletion     InternalKeyKind = 4
	//InternalKeyKindColumnFamilyValue        InternalKeyKind = 5
	//InternalKeyKindColumnFamilyMerge        InternalKeyKind = 6

	// InternalKeyKindSingleDelete (SINGLEDEL) is a performance optimization
	// solely for compactions (to reduce write amp and space amp). Readers other
	// than compactions should treat SINGLEDEL as equivalent to a DEL.
	// Historically, it was simpler for readers other than compactions to treat
	// SINGLEDEL as equivalent to DEL, but as of the introduction of
	// InternalKeyKindSSTableInternalObsoleteBit, this is also necessary for
	// correctness.
	InternalKeyKindSingleDelete InternalKeyKind = 7
	//InternalKeyKindColumnFamilySingleDelete InternalKeyKind = 8
	//InternalKeyKindBeginPrepareXID          InternalKeyKind = 9
	//InternalKeyKindEndPrepareXID            InternalKeyKind = 10
	//InternalKeyKindCommitXID                InternalKeyKind = 11
	//InternalKeyKindRollbackXID              InternalKeyKind = 12
	//InternalKeyKindNoop                     InternalKeyKind = 13
	//InternalKeyKindColumnFamilyRangeDelete  InternalKeyKind = 14
	InternalKeyKindRangeDelete InternalKeyKind = 15
	//InternalKeyKindColumnFamilyBlobIndex    InternalKeyKind = 16
	//InternalKeyKindBlobIndex                InternalKeyKind = 17

	// InternalKeyKindSeparator is a key used for separator / successor keys
	// written to sstable block indexes.
	//
	// NOTE: the RocksDB value has been repurposed. This was done to ensure that
	// keys written to block indexes with value "17" (when 17 happened to be the
	// max value, and InternalKeyKindMax was therefore set to 17), remain stable
	// when new key kinds are supported in Pebble.
	InternalKeyKindSeparator InternalKeyKind = 17

	// InternalKeyKindSetWithDelete keys are SET keys that have met with a
	// DELETE or SINGLEDEL key in a prior compaction. This key kind is
	// specific to Pebble. See
	// https://github.com/cockroachdb/pebble/issues/1255.
	InternalKeyKindSetWithDelete InternalKeyKind = 18

	// InternalKeyKindRangeKeyDelete removes all range keys within a key range.
	// See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyDelete InternalKeyKind = 19
	// InternalKeyKindRangeKeySet and InternalKeyKindRangeUnset represent
	// keys that set and unset values associated with ranges of key
	// space. See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyUnset InternalKeyKind = 20
	InternalKeyKindRangeKeySet   InternalKeyKind = 21

	InternalKeyKindRangeKeyMin InternalKeyKind = InternalKeyKindRangeKeyDelete
	InternalKeyKindRangeKeyMax InternalKeyKind = InternalKeyKindRangeKeySet

	// InternalKeyKindIngestSST is used to distinguish a batch that corresponds to
	// the WAL entry for ingested sstables that are added to the flushable
	// queue. This InternalKeyKind cannot appear amongst other key kinds in a
	// batch (with the exception of alongside InternalKeyKindExcise), or in an sstable.
	InternalKeyKindIngestSST InternalKeyKind = 22

	// InternalKeyKindDeleteSized keys behave identically to
	// InternalKeyKindDelete keys, except that they hold an associated uint64
	// value indicating the (len(key)+len(value)) of the shadowed entry the
	// tombstone is expected to delete. This value is used to inform compaction
	// heuristics, but is not required to be accurate for correctness.
	InternalKeyKindDeleteSized InternalKeyKind = 23

	// InternalKeyKindExcise is used to persist the Excise part of an IngestAndExcise
	// to a WAL. An Excise is similar to a RangeDel+RangeKeyDel combined, in that it
	// deletes all point and range keys in a given key range while also immediately
	// truncating sstables to exclude this key span. This InternalKeyKind cannot
	// appear amongst other key kinds in a batch (with the exception of alongside
	// InternalKeyKindIngestSST), or in an sstable.
	InternalKeyKindExcise InternalKeyKind = 24

	// This maximum value isn't part of the file format. Future extensions may
	// increase this value.
	//
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 24

	// InternalKeyKindMaxForSSTable is the largest valid key kind that can exist
	// in an SSTable. This should usually equal InternalKeyKindMax, except
	// if the current InternalKeyKindMax is a kind that is never added to an
	// SSTable or memtable (eg. InternalKeyKindExcise).
	InternalKeyKindMaxForSSTable InternalKeyKind = InternalKeyKindDeleteSized

	// Internal to the sstable format. Not exposed by any sstable iterator.
	// Declared here to prevent definition of valid key kinds that set this bit.
	InternalKeyKindSSTableInternalObsoleteBit  InternalKeyKind = 64
	InternalKeyKindSSTableInternalObsoleteMask InternalKeyKind = 191

	// A marker for an invalid key.
	InternalKeyKindInvalid InternalKeyKind = InternalKeyKindSSTableInternalObsoleteMask
)

// DecodeInternalKey decodes an encoded internal key. See InternalKey.Encode().
func DecodeInternalKey(encodedKey []byte) InternalKey {
	n := len(encodedKey) - InternalTrailerLen
	var trailer InternalKeyTrailer
	if n >= 0 {
		trailer = InternalKeyTrailer(binary.LittleEndian.Uint64(encodedKey[n:]))
		encodedKey = encodedKey[:n:n]
	} else {
		trailer = InternalKeyTrailer(InternalKeyKindInvalid)
		encodedKey = nil
	}
	return InternalKey{
		UserKey: encodedKey,
		Trailer: trailer,
	}
}
