package log

import (
	"encoding/binary" // For encoding/decoding uint32 and uint64 in big-endian format.
	"fmt"             // For wrapping errors with context.
	"io"              // For io.EOF errors.
	"os"              // For file operations.

	"github.com/tysonmote/gommap" // For memory-mapped file operations.
)

// Constants defining the structure of an index entry.
// Each entry maps a record's offset (relative to a segment) to its position in the store file.
const (
	offWidth uint64 = 4                   // Size of the offset field (uint32, 4 bytes).
	posWidth uint64 = 8                   // Size of the position field (uint64, 8 bytes).
	entWidth        = offWidth + posWidth // Total size of an entry (12 bytes).
)

// index manages a memory-mapped file containing fixed-size entries (offset, position).
// It maps record numbers to their positions in the corresponding store file for fast lookups.
type index struct {
	file *os.File    // Underlying file handle for the index.
	mmap gommap.MMap // Memory-mapped view of the file for efficient access.
	size uint64      // Current size of the index file (in bytes).
}

// newIndex creates a new index for the given file, limited by the max size in config.
// It truncates the file to MaxIndexBytes and maps it into memory.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	// Get the current file size to initialize the size tracker.
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to stat index file %s: %w", f.Name(), err)
	}
	idx.size = uint64(fi.Size())

	// Truncate the file to the maximum allowed size to prevent uncontrolled growth.
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, fmt.Errorf("failed to truncate index file %s to %d bytes: %w", f.Name(), c.Segment.MaxIndexBytes, err)
	}

	// Map the file into memory for efficient read/write access.
	// Uses PROT_READ|PROT_WRITE for read-write access and MAP_SHARED to sync changes to disk.
	if idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, fmt.Errorf("failed to memory-map index file %s: %w", f.Name(), err)
	}
	return idx, nil
}

// Close ensures all changes are synced to disk and closes the file.
// It syncs the memory-mapped region, truncates to the actual size, and closes the file.
func (i *index) Close() error {
	// Sync the memory-mapped region to disk to ensure all writes are persisted.
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf("failed to sync memory-mapped index: %w", err)
	}
	// Sync the file to ensure metadata (e.g., size) is updated.
	if err := i.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync index file %s: %w", i.file.Name(), err)
	}
	// Truncate the file to the actual used size to avoid wasted space.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return fmt.Errorf("failed to truncate index file %s to size %d: %w", i.file.Name(), i.size, err)
	}
	// Close the underlying file handle.
	if err := i.file.Close(); err != nil {
		return fmt.Errorf("failed to close index file %s: %w", i.file.Name(), err)
	}
	return nil
}

// Read retrieves an index entry by its entry number (in).
// If in = -1, reads the last entry; otherwise, reads the entry at index in.
// Returns the offset (relative to segment), position (in store file), and any error.
func (i *index) Read(in int64) (offset uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF // Empty index means no entries.
	}

	// Calculate the entry number to read.
	var entryNum uint32
	if in == -1 {
		// Read the last entry (size / entWidth gives number of entries).
		entryNum = uint32((i.size / entWidth) - 1)
	} else {
		entryNum = uint32(in) // Read the specific entry number.
	}

	// Calculate the byte offset in the memory-mapped file for this entry.
	entryPos := uint64(entryNum) * entWidth
	if i.size < entryPos+entWidth {
		return 0, 0, io.EOF // Beyond file size, no such entry.
	}

	// Read the offset (4 bytes) and position (8 bytes) from the memory-mapped file.
	offset = binary.BigEndian.Uint32(i.mmap[entryPos : entryPos+offWidth])
	pos = binary.BigEndian.Uint64(i.mmap[entryPos+offWidth : entryPos+entWidth])
	return offset, pos, nil
}

// Write appends a new index entry with the given offset and position.
// Returns an error if the memory-mapped region is too small.
func (i *index) Write(offset uint32, pos uint64) error {
	// Check if there's enough space in the memory-mapped region for a new entry.
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF // Exceeds max index size.
	}

	// Write the offset and position to the memory-mapped file at the current size.
	binary.BigEndian.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	binary.BigEndian.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth // Update the size tracker.
	return nil
}

// Name returns the name of the underlying index file.
func (i *index) Name() string {
	return i.file.Name()
}
