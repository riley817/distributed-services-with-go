package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// 레코드 크기와 인덱스 항목을 저장할 때 인코딩 정의
	enc = binary.BigEndian
)

const (
	// 레코드 길이를 저장하는 바이트 개수8
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append 바이트 슬라이스를 입력받아 저장 파일에 쓴다.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// 저장파일에 쓴다.
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w) // 나중에 얼마나 읽어야 할지 알 수 있도록 레코드 크기도 쓴다.
	return uint64(w), pos, nil
}

// Read 해당 위치에 저장된 레코드를 읽는다.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 읽으려는 레코드가 아직 버퍼에 있을 때를 대비하여 우선은 쓰기 버퍼의 내용을 flush 한다.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// 다음으로 읽을 레코드의 바이트 크기를 알아내고 그 크기만큼 읽는다.
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt 저장 파일에서 off 부터 len(p) 바이트를 읽어 p 에 저장한다.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close 버퍼를 닫기전 버퍼의 데이터를 파일에 쓴다.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
