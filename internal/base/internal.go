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
