package log

import (
	"fmt"
	"os"
	"path"
)

// segment store와 index를 감싸는 타입
// segment는 데이터를 스토어에 쓰고 새로운 인덱스 항목을 인덱스에 추가
// segment는 레코드를 읽고 인덱스를 검색
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64 // 베이스 오프셋과 다음에 추가할 오프셋
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {

	}
}
