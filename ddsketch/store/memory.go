package store

// memoryPool helps to reuse already allocated byte slices.
// This is not thread-safe. All calls to its methods should be single-threaded.
type memoryPool struct {
	blocks        [][]byte
	blockSizeLog2 uint8
}

func newUnlimitedMemoryPool(blockSizeLog2 uint8) *memoryPool {
	return &memoryPool{
		blocks:        nil,
		blockSizeLog2: blockSizeLog2,
	}
}

// acquire behaves similarly to make([]byte, blockSize) from the caller's point
// of view.
func (p *memoryPool) acquire() []byte {
	if len(p.blocks) > 0 {
		b := p.blocks[len(p.blocks)-1]
		p.blocks = p.blocks[:len(p.blocks)-1]

		// If the capacity is >= 4 times the length, halve the capacity.
		if cap(p.blocks)>>1 >= 8 && cap(p.blocks)>>2 >= len(p.blocks) {
			p.blocks = (p.blocks)[: len(p.blocks) : cap(p.blocks)>>1]
		}

		for i := range b {
			b[i] = 0
		}
		return b
	} else {
		return make([]byte, 1<<p.blockSizeLog2)
	}
}

func (p *memoryPool) release(b []byte) {
	if cap(b) != int(1<<p.blockSizeLog2) {
		panic("invalid block size")
	}

	p.blocks = append(p.blocks, b[:cap(b)])
}
