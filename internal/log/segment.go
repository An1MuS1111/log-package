package log

import (
	"fmt"                       // For error wrapping and file path formatting.
	log_v1 "log-package/api/v1" // For the Record proto definition.
	"os"                        // For file operations.
	"path"                      // For constructing file paths.

	"google.golang.org/protobuf/proto" // For serializing/deserializing records.
)

// segment combines a store and index to manage a log segment.
// A segment consists of a store file (for record data) and an index file (for offset-to-position mappings).
// It tracks the base offset (first record's offset) and next offset (for appending).
type segment struct {
	store                  *Store // Store for serialized records.
	index                  *index // Index for mapping offsets to store positions.
	baseOffset, nextOffset uint64 // Base offset of the segment; next offset to assign.
	config                 Config // Configuration for size limits and other settings.
}

// newSegment creates a new segment in the specified directory with the given base offset.
// It initializes the store and index files, named by the base offset (e.g., "123.store", "123.index").
// The next offset is set based on the last index entry, if any.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// Open or create the store file with read/write, append, and create flags.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open store file %s: %w", path.Join(dir, fmt.Sprintf("%d.store", baseOffset)), err)
	}

	// Initialize the store.
	if s.store, err = NewStore(storeFile); err != nil {
		return nil, fmt.Errorf("failed to initialize store for segment %d: %w", baseOffset, err)
	}

	// Open or create the index file.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", path.Join(dir, fmt.Sprintf("%d.index", baseOffset)), err)
	}

	// Initialize the index with the config (e.g., MaxIndexBytes).
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, fmt.Errorf("failed to initialize index for segment %d: %w", baseOffset, err)
	}

	// Read the last index entry to determine the next offset.
	// If no entries exist, start at baseOffset; otherwise, increment the last offset.
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset // No entries, start at base offset.
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1 // Continue from last offset + 1.
	}

	return s, nil
}

// Append adds a record to the segment, assigning it the next offset.
// It serializes the record using Protocol Buffers, appends it to the store,
// and adds an index entry mapping the relative offset to the store position.
// Returns the assigned offset and any error.
func (s *segment) Append(record *log_v1.Record) (offset uint64, err error) {
	// Use the next available offset for this record.
	cur := s.nextOffset
	record.Offset = cur // Set the record's offset field for consistency.

	// Serialize the record to bytes using Protocol Buffers.
	p, err := proto.Marshal(record)
	if err != nil {
		// return 0, fmt.Errorf("failed to serialize record at offset %d: %w", cur, err)
		return 0, err
	}

	// Append the serialized record to the store, getting its position.
	_, pos, err := s.store.Append(p)
	if err != nil {
		// return 0, fmt.Errorf("failed to append record to store at offset %d: %w", cur, err)
		return 0, err
	}

	// Write an index entry with the relative offset (current offset - base offset).
	// The position (pos) points to the record's location in the store file.
	if err = s.index.Write(uint32(s.nextOffset-uint64(s.baseOffset)), pos); err != nil {
		// return 0, fmt.Errorf("failed to write index entry for offset %d: %w", cur, err)
		return 0, err
	}

	// Increment the next offset for the next record.
	s.nextOffset++

	return cur, nil
}

// Read retrieves a record from the segment by its offset.
// It uses the index to find the store position, reads the serialized record,
// and deserializes it into a log_v1.Record.
// Returns the record and any error.
func (s *segment) Read(off uint64) (*log_v1.Record, error) {
	// Calculate the relative offset for the index (offset - base offset).
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, fmt.Errorf("failed to read index entry for offset %d: %w", off, err)
	}

	// Read the serialized record from the store at the given position.
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read record from store at position %d: %w", pos, err)
	}

	// Deserialize the record.
	record := &log_v1.Record{}
	if err = proto.Unmarshal(p, record); err != nil {
		return nil, fmt.Errorf("failed to deserialize record at offset %d: %w", off, err)
	}

	return record, nil
}

// IsMaxed checks if the segment has reached its size limits.
// Returns true if either the store or index exceeds its maximum configured size.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove deletes the segment's store and index files after closing them.
// Returns any error encountered during closing or removal.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return fmt.Errorf("failed to close segment %d: %w", s.baseOffset, err)
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return fmt.Errorf("failed to remove index file %s: %w", s.index.Name(), err)
	}
	if err := os.Remove(s.store.file.Name()); err != nil {
		return fmt.Errorf("failed to remove store file %s: %w", s.store.file.Name(), err)
	}
	return nil
}

// Close closes the segment by closing both the store and index.
// Ensures all data is flushed and resources are released.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return fmt.Errorf("failed to close index for segment %d: %w", s.baseOffset, err)
	}
	if err := s.store.Close(); err != nil {
		return fmt.Errorf("failed to close store for segment %d: %w", s.baseOffset, err)
	}
	return nil
}

// nearestMultiple rounds j down to the nearest multiple of k.
// Used for aligning offsets or sizes, though not used in this segment code.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
