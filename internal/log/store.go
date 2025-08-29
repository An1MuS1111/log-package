package log

import (
	"bufio"           // For buffered writing to improve performance on frequent small writes.
	"encoding/binary" // For encoding/decoding the length prefix as big-endian uint64.
	"fmt"             // For wrapping errors with more context.
	"os"              // For file operations.
	"sync"            // For mutex to ensure thread safety.
)

// Constants for clarity and to avoid magic numbers.
// LengthPrefixSize defines the fixed size (in bytes) of the record length prefix.
// We use 8 bytes for a uint64, which can handle very large records (up to ~18 exabytes).
const (
	LengthPrefixSize = 8
)

// Store represents a thread-safe, buffered file store for appending and reading length-prefixed records.
// It tracks the file size internally for quick position calculations.
type Store struct {
	file   *os.File      // Underlying file handle.
	writer *bufio.Writer // Buffered writer for efficient appends.
	mutex  sync.Mutex    // Mutex for synchronizing access (reads/writes).
	size   uint64        // Current size of the file (updated on appends).
}

// NewStore initializes a new Store with the given file.
// It stats the file to get its initial size and sets up buffering.
func NewStore(file *os.File) (*Store, error) {
	info, err := file.Stat()
	if err != nil {
		// Wrap the error for better debugging (e.g., "failed to get file info: permission denied").
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	return &Store{
		file:   file,
		writer: bufio.NewWriter(file), // Buffer writes to reduce disk I/O calls.
		size:   uint64(info.Size()),   // Start with the existing file size.
	}, nil
}

// Append adds a new record to the end of the file.
// It writes a length prefix (uint64) followed by the data.
// Returns: bytes written (prefix + data), starting position of the record, and any error.
// This method is thread-safe due to the mutex.
func (s *Store) Append(data []byte) (bytesWritten uint64, position uint64, err error) {
	s.mutex.Lock()         // Lock to prevent concurrent writes/reads.
	defer s.mutex.Unlock() // Unlock after the operation (even on error).

	position = s.size // Record starts at the current file size.

	// Write the length prefix using big-endian encoding for consistency across platforms.
	if err := binary.Write(s.writer, binary.BigEndian, uint64(len(data))); err != nil {
		return 0, 0, fmt.Errorf("failed to write length prefix: %w", err)
	}

	// Write the actual data bytes.
	written, err := s.writer.Write(data)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write data: %w", err)
	}

	// Calculate total bytes written: prefix size + data length.
	bytesWritten = uint64(written + LengthPrefixSize)
	s.size += bytesWritten // Update internal size tracker.

	return bytesWritten, position, nil
}

// Read retrieves a full record starting at the given position.
// It first reads the length prefix, then reads exactly that many bytes.
// Flushes the writer buffer first to ensure data is committed to disk.
// Returns: the record data bytes and any error.
// This method is thread-safe.
func (s *Store) Read(position uint64) ([]byte, error) {
	s.mutex.Lock()         // Lock for safety.
	defer s.mutex.Unlock() // Unlock after.

	// Flush any buffered writes to ensure the file is up-to-date for reading.
	if err := s.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush writer before read: %w", err)
	}

	// Read the length prefix (fixed 8 bytes).
	lengthBuf := make([]byte, LengthPrefixSize)
	if _, err := s.file.ReadAt(lengthBuf, int64(position)); err != nil {
		return nil, fmt.Errorf("failed to read length prefix at position %d: %w", position, err)
	}
	dataLength := binary.BigEndian.Uint64(lengthBuf) // Decode the length.

	// Allocate and read the data bytes based on the length.
	data := make([]byte, dataLength)
	if _, err := s.file.ReadAt(data, int64(position+LengthPrefixSize)); err != nil {
		return nil, fmt.Errorf("failed to read data at position %d: %w", position+LengthPrefixSize, err)
	}

	return data, nil
}

// ReadAt reads arbitrary bytes from the file at a specific offset into the provided buffer.
// Unlike Read, this doesn't assume a record structure—it's a low-level read.
// Useful for custom or partial reads.
// Flushes the writer first for consistency.
// Returns: number of bytes read and any error.
// Thread-safe.
func (s *Store) ReadAt(data []byte, offset int64) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.writer.Flush(); err != nil {
		return 0, fmt.Errorf("failed to flush writer before read: %w", err)
	}

	n, err := s.file.ReadAt(data, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to read at offset %d: %w", offset, err)
	}
	return n, nil
}

// Close flushes any remaining buffered data to disk and closes the file.
// Thread-safe; should be called when done with the store to free resources.
func (s *Store) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Flush buffer to ensure all data is written.
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer on close: %w", err)
	}
	// Close the underlying file.
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	return nil
}
