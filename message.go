package	gearman

import	(
	"io"
)


type work_writer func([]byte) (int, error)


func (f work_writer) Write(p []byte) (int, error) {
	return f(p)
}


type message struct {
	pool	*pool
	server	Conn
	pkt	Packet
}


func (msg message) pkt_reply( pkt Packet ) {
	msg.pool.send_to(msg.server, pkt )
}


func (msg message) reply( c Command, d ...[]byte ) {
	msg.pool.send_to(msg.server, packet(c, append([][]byte{ msg.pkt.At(0) }, d...)... ))
}


func (msg message) work_data() io.Writer {
	return work_writer(func(p []byte) (n int, err error){
		msg.pool.send_to(msg.server, packet(WORK_DATA_WRK, msg.pkt.At(0), p ))
		return len(p),nil
	})
}


func (msg message) work_complete() io.Writer {
	return work_writer(func(p []byte) (n int, err error){
		msg.pool.send_to(msg.server, packet(WORK_COMPLETE_WRK, msg.pkt.At(0), p ))
		return len(p),nil
	})
}
