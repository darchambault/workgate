// Keyboard input for the monitor's live view.
//
// Only the handful of keys the monitor binds are decoded, so no terminal
// library is pulled in here either. Putting the console into a mode where
// keystrokes arrive one at a time is platform specific and lives in
// term_windows.go and term_unix.go, beside the output half.
package main

import "os"

// key is one keystroke the monitor understands. keyNone is the answer for
// most bytes, which is the point: an unbound key must do nothing at all.
type key int

const (
	keyNone key = iota
	keyUp
	keyDown
	keyLeft
	keyRight
	keyClear // drop the selection
	keyQuit
)

// Decoder states.
const (
	keyGround  = iota // between sequences
	keyEscaped        // read an ESC, waiting to see what it introduces
	keyBracket        // read the CSI or SS3 introducer, waiting for the final byte
)

// keyDecoder turns a byte stream into keys.
//
// It is a state machine over single bytes rather than a match over a buffer
// because an escape sequence is three bytes, not one packet: a slow link can
// split it across two reads. A fresh ESC restarts the machine from any state,
// so a truncated sequence cannot swallow the one that follows it.
//
// Both arrow encodings are accepted. CSI (\x1b[A) is what a terminal sends by
// default; SS3 (\x1bOA) is what it sends in application cursor mode, which
// tmux and some emulators use whatever the monitor asked for.
type keyDecoder struct{ state int }

// next feeds one byte and returns the key it completed, or keyNone.
func (d *keyDecoder) next(b byte) key {
	if b == 0x1b {
		d.state = keyEscaped
		return keyNone
	}
	switch d.state {
	case keyEscaped:
		if b == '[' || b == 'O' {
			d.state = keyBracket
		} else {
			d.state = keyGround
		}
		return keyNone
	case keyBracket:
		d.state = keyGround
		switch b {
		case 'A':
			return keyUp
		case 'B':
			return keyDown
		case 'C':
			return keyRight
		case 'D':
			return keyLeft
		}
		// A parameterized sequence (Ctrl+Up is \x1b[1;5A), a mouse report, a
		// bracketed paste: the introducer is consumed and the rest reads as
		// ordinary bytes, none of which are bound.
		return keyNone
	}
	// The vim keys are the arrows' second names, mapped the same way: h and l
	// move the level as left and right do, j and k move the selection.
	switch b {
	case 'k':
		return keyUp
	case 'j':
		return keyDown
	case 'h':
		return keyLeft
	case 'l':
		return keyRight
	case 'q', 'Q':
		return keyQuit
	}
	return keyNone
}

// pendingEscape reports whether the decoder is holding a bare ESC.
//
// A lone Esc and the start of an arrow sequence are the same byte, and telling
// them apart needs either a timer or an assumption. The assumption is cheaper
// and is right on every terminal worth naming: an escape sequence is written
// in one go, so an ESC still pending at the end of a read really was Esc. The
// cost of being wrong is a cleared selection and one ignored arrow key.
func (d *keyDecoder) pendingEscape() bool { return d.state == keyEscaped }

// reset returns the decoder to the ground state.
func (d *keyDecoder) reset() { d.state = keyGround }

// keysBuffered is how many keystrokes may be in flight.
//
// A held-down arrow autorepeats faster than a frame can be drawn, and the
// reader drops rather than blocks once this many are queued: more than any
// human burst, less than any autorepeat storm. Dropping the overflow is the
// right answer — a queue is not a text field, and the keystrokes that matter
// are the ones the user is still watching the screen for.
const keysBuffered = 16

// keyReader delivers keystrokes and restores the console mode it changed.
// The zero value is a disabled reader, which is what a redirected monitor and
// a terminal too old for this both get.
type keyReader struct {
	ch      chan key
	restore func()
	enabled bool
}

// newKeyReader puts in into a mode where keystrokes arrive unbuffered and
// unechoed, and starts reading it.
//
// It returns a disabled reader when in is not a terminal, when the mode cannot
// be changed, or when the monitor's own output is redirected: frames going to
// a pipe must stay exactly the read-only view they have always been, and keys
// must not be offered where there is no live screen to show their result on.
func newKeyReader(in *os.File, outIsTTY bool) *keyReader {
	if !outIsTTY {
		return &keyReader{}
	}
	ok, restore := enableKeys(in)
	if !ok {
		return &keyReader{}
	}
	r := &keyReader{ch: make(chan key, keysBuffered), restore: restore, enabled: true}
	go readKeys(in, r.ch)
	return r
}

// keys is the channel the monitor selects on. A disabled reader returns nil,
// and a receive from a nil channel blocks forever — exactly the select arm
// that should never fire.
func (r *keyReader) keys() <-chan key {
	if r == nil {
		return nil
	}
	return r.ch
}

// close restores the console mode, once.
func (r *keyReader) close() {
	if r == nil || r.restore == nil {
		return
	}
	r.restore()
	r.restore = nil
}

// readKeys reads in until it fails, forwarding the keys it recognises.
//
// The read itself is readInput, which is platform specific: a Windows console
// cannot be read as a file here, and the difference is invisible from this
// side. Everything after the read is shared, because the bytes are the same
// escape sequences on both.
//
// This goroutine is never stopped, and the channel is never closed. Stdin
// cannot be interrupted portably — closing it is worse than leaking it, and a
// blocking console read on Windows does not notice a close at all — so the
// reader is left blocked and dies with the process. That is safe because the
// send is non-blocking: an abandoned reader parks in a read holding nothing,
// rather than parking on a send nobody will ever drain. Closing the channel
// would be worse still: the monitor's select arm would then fire forever.
func readKeys(in *os.File, out chan<- key) {
	var d keyDecoder
	buf := make([]byte, 64)
	send := func(k key) {
		select {
		case out <- k:
		default:
		}
	}
	for {
		n, err := readInput(in, buf)
		for _, b := range buf[:n] {
			if k := d.next(b); k != keyNone {
				send(k)
			}
		}
		if n > 0 && d.pendingEscape() {
			d.reset()
			send(keyClear)
		}
		if err != nil {
			return
		}
	}
}
