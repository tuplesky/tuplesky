// Package pin keeps the reviewed quic-go candidate (design Section 16.1) in the
// module graph so go.sum records its checksum before task-45 uses it.
// Nothing here opens sockets.
package pin

import quic "github.com/quic-go/quic-go"

// QUICVersion is the QUIC version the client will negotiate; RFC 9000 v1.
const QUICVersion = quic.Version1
