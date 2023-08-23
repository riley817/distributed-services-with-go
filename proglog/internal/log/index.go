package log

import (
	"github.com/tysonmote/gommap"
	"io"
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
	// 마지막 인덱스만큼 잘라내 마지막 인덱스 항목이 파일의 끝부분에 있도록 만든다음 서비스를 종료
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read 매개변수를 오프셋으로 받아서 해당하는 레코드의 저장 파일 내 위치를 리턴 - 오프셋은 해당 세그먼트의 베이스 오프셋(?)
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// 인덱스의 첫 항목의 오프셋은 항상 0
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32(i.size/entWidth - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWith])
	pos = enc.Uint64(i.mmap[pos+offWith : pos+entWidth])
	return out, pos, nil
}

// Write 오프셋과 위치를 매개변수로 받아 인덱스에 추가
func (i *index) Write(off uint32, pos uint64) error {
	// 추가할 공간이 있는지 확인
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// 추가할 공간이 있다면 인코딩 후 다음 메모리 맵 팡리에 쓴다.
	enc.PutUint32(i.mmap[i.size:i.size+offWith], off)
	enc.PutUint64(i.mmap[i.size+offWith:i.size+entWidth], pos)

	// size를 증가시켜 다음에 쓸 위치를 가리키게 한다.
	i.size += entWidth
	return nil
}

// Name 인덱스 파일의 경로를 반환
func (i *index) Name() string {
	return i.file.Name()
}
