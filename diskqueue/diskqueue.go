package diskqueue

import (
	"os"
	"errors"
	"bytes"
	"bufio"
	"fmt"
	"math/rand"
	"encoding/binary"
	"io"
	"path"
	"time"
)

type DiskQueue struct {
	name string
	readPos int64
	writePos int64
	readFileNum int64
	writeFileNum int64
	
	dataPath string
	maxBytesPerFile int64
	
	nextReadPos int64
	nextReadFileNum int64
	
	readFile *os.File
	writeFile *os.File
	reader *bufio.Reader
	writeBuf bytes.Buffer
	needSync bool
	needSyncCh chan bool
	syncEvery uint32
	size int
}

func NewDiskQueue (name string, dataPath string) *DiskQueue {
	q := &DiskQueue{
		name: name,
		dataPath: dataPath,
		maxBytesPerFile: 1000000,
		needSync: false,
	}
	q.retrieveMetaData()
	go q.sync()
	return q
}

func (q *DiskQueue) Size() int {
	return q.size
}

func (q *DiskQueue) Put(v interface{}) error {
	b, ok := v.([]byte);
	if !ok {
		return errors.New("v must be a []byte")
	}
	var err error
	if q.writeFile == nil {
		curFileName := q.fileName(q.writeFileNum)
		q.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		if q.writePos > 0 {
			_, err = q.writeFile.Seek(q.writePos, 0)
			if err != nil {
				q.writeFile.Close()
				q.writeFile = nil
				return err
			}
		}
	}
	dataLen := int32(len(b))
	q.writeBuf.Reset()
	err = binary.Write(&q.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}
	_, err = q.writeBuf.Write(b)
	if err != nil {
		return err
	}
	_, err = q.writeFile.Write(q.writeBuf.Bytes())
	if err != nil {
		q.writeFile.Close()
		q.writeFile = nil
		return err
	}
	q.writeFile.Sync()
	totalBytes := int64(4 + dataLen)
	q.writePos += totalBytes

	if q.writePos > q.maxBytesPerFile {
		q.writeFileNum++
		q.writePos = 0
		//q.sync()

		if q.writeFile != nil {
			q.writeFile.Close()
			q.writeFile = nil
		}
	}
	q.size++
	return err
}

func (q *DiskQueue) Get() (interface{}, error) {
	if q.size == 0 {
		return nil, errors.New("empty queue")
	}
	if q.readFileNum > q.writeFileNum || (q.readFileNum == q.writeFileNum && q.readPos >= q.writePos) {
	    return nil, errors.New("empty queue")
	}
	var err error
	var msgSize int32
	if q.readFile == nil {
		curFileName := q.fileName(q.readFileNum)
		q.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		if q.readPos > 0 {
			_, err = q.readFile.Seek(q.readPos, 0)
			if err != nil {
				q.readFile.Close()
				q.readFile = nil
				return nil, err
			}
		}

		q.reader = bufio.NewReader(q.readFile)
	}
	err = binary.Read(q.reader, binary.BigEndian, &msgSize)
	if err != nil {
		q.readFile.Close()
		q.readFile = nil
		return nil, err
	}
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(q.reader, readBuf)
	if err != nil {
		q.readFile.Close()
		q.readFile = nil
		return nil, err
	}
	totalBytes := int64(4 + msgSize)
	
	q.nextReadPos = q.readPos + totalBytes
	q.nextReadFileNum = q.readFileNum

	if q.nextReadPos > q.maxBytesPerFile {
		if q.readFile != nil {
			q.readFile.Close()
			q.readFile = nil
		}

		q.nextReadFileNum++
		q.nextReadPos = 0
	}
	q.moveForward()
	q.size--
	return readBuf, nil
}

func (q *DiskQueue) moveForward() {
	q.readFileNum = q.nextReadFileNum
	q.readPos = q.nextReadPos
}

func (q *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := q.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d,%d,%d,%d,%d",
		&q.size,
		&q.readFileNum, &q.readPos,
		&q.writeFileNum, &q.writePos)
	if err != nil {
		return err
	}
	
	q.nextReadFileNum = q.readFileNum
	q.nextReadPos = q.readPos
	size := q.size
	q.size = 1
	for v, err := q.Get(); err == nil; {
		q.size++
		size++
	}
	q.writeFileNum = q.readFileNum
	q.writePos = q.nextReadPos
	q.size = size
	return nil
}

func (q *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := q.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d,%d,%d,%d,%d",
		q.size,
		q.readFileNum, q.readPos,
		q.writeFileNum, q.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	return os.Rename(tmpFileName, fileName)
}

func (q *DiskQueue) sync() error {
	for {
		q.persistMetaData()
		time.Sleep(time.Second)
	}
}

func (q *DiskQueue) sendSyncSignal() {
	q.needSync = true
	select {
		case q.needSyncCh<-true:
		default:
	}
}

func (q *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(q.dataPath, "%s.diskqueue.meta.dat"), q.name)
}

func (q *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(q.dataPath, "%s.diskqueue.%06d.dat"), q.name, fileNum)
}