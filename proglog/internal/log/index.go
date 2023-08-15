package log

import (
	"github.com/tysonmote/gommap"
	"os"
)

var (
	offWith  uint64 = 4                  // 레코드 오프셋(4 byte)
	posWidth uint64 = 8                  // 스토어 파일 위치(8 byte)
	entWidth        = offWith + posWidth // 오프셋이 가리키는 위치
)

// 인덱스 파일을 정의
type index struct {
	file *os.File
	mmap gommap.MMap // 메모리 맵
	size uint64      // 인덱스에 다음 항목을 추가할 위치
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name()) // 파일의 크기를 저장한다. 인덱스를 저장하면서 인데스 파일의 데이터양을 추적
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.maxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// 메모리 맵과 파일의 데이터를 동기화 한다.
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
