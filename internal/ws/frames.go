package ws

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

func readFrame(conn net.Conn) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, err
	}

	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	payloadLen := int64(header[1] & 0x7F)

	if !fin {
		return 0, nil, fmt.Errorf("fragmented frames unsupported")
	}
	if !masked {
		return 0, nil, fmt.Errorf("client frames must be masked")
	}

	switch payloadLen {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(conn, ext[:]); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext[:]))
	}

	var maskKey [4]byte
	if _, err := io.ReadFull(conn, maskKey[:]); err != nil {
		return 0, nil, err
	}

	if payloadLen > (1 << 20) { // 1MB guard to avoid memory abuse
		return 0, nil, fmt.Errorf("frame too large")
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, err
	}
	for i := int64(0); i < payloadLen; i++ {
		payload[i] ^= maskKey[i%4]
	}

	return opcode, payload, nil
}

func writeFrame(conn net.Conn, opcode byte, payload []byte, timeout time.Duration) error {
	if payload == nil {
		payload = []byte{}
	}
	header := []byte{0x80 | opcode}
	length := len(payload)

	switch {
	case length < 126:
		header = append(header, byte(length))
	case length <= 0xFFFF:
		header = append(header, 126)
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], uint16(length))
		header = append(header, buf[:]...)
	default:
		header = append(header, 127)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(length))
		header = append(header, buf[:]...)
	}

	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if length > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Time{})
	}
	return nil
}
