package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	ss "github.com/shadowsocks/shadowsocks-go/shadowsocks"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
)

var debug ss.DebugLog

var (
	errAddrType      = errors.New("socks addr type not supported")
	errVer           = errors.New("socks version not supported")
	errMethod        = errors.New("socks only support 1 method now")
	errAuthExtraData = errors.New("socks authentication get extra data")
	errReqExtraData  = errors.New("socks request get extra data")
	errCmd           = errors.New("socks command not supported")
	remoteSites      = NewSiteCache()
)

const (
	socksVer5       = 5
	socksCmdConnect = 1
)

func init() {
	rand.Seed(time.Now().Unix())
}

func handShake(conn net.Conn) (err error) {
	const (
		idVer     = 0
		idNmethod = 1
	)
	// version identification and method selection message in theory can have
	// at most 256 methods, plus version and nmethod field in total 258 bytes
	// the current rfc defines only 3 authentication methods (plus 2 reserved),
	// so it won't be such long in practice

	buf := make([]byte, 258)

	var n int
	// make sure we get the nmethod field
	if n, err = io.ReadAtLeast(conn, buf, idNmethod+1); err != nil {
		return
	}
	if buf[idVer] != socksVer5 {
		return errVer
	}
	nmethod := int(buf[idNmethod])
	msgLen := nmethod + 2
	if n == msgLen { // handshake done, common case
		// do nothing, jump directly to send confirmation
	} else if n < msgLen { // has more methods to read, rare case
		if _, err = io.ReadFull(conn, buf[n:msgLen]); err != nil {
			return
		}
	} else { // error, should not get extra data
		return errAuthExtraData
	}
	// send confirmation: version 5, no authentication required
	_, err = conn.Write([]byte{socksVer5, 0})
	return
}

func getRequest(conn net.Conn) (rawaddr []byte, addr string, host string, err error) {
	const (
		idVer   = 0
		idCmd   = 1
		idType  = 3 // address type index
		idIP0   = 4 // ip addres start index
		idDmLen = 4 // domain address length index
		idDm0   = 5 // domain address start index

		typeIPv4 = 1 // type is ipv4 address
		typeDm   = 3 // type is domain address
		typeIPv6 = 4 // type is ipv6 address

		lenIPv4   = 3 + 1 + net.IPv4len + 2 // 3(ver+cmd+rsv) + 1addrType + ipv4 + 2port
		lenIPv6   = 3 + 1 + net.IPv6len + 2 // 3(ver+cmd+rsv) + 1addrType + ipv6 + 2port
		lenDmBase = 3 + 1 + 1 + 2           // 3 + 1addrType + 1addrLen + 2port, plus addrLen
	)
	// refer to getRequest in server.go for why set buffer size to 263
	buf := make([]byte, 263)
	var n int
	// read till we get possible domain length field
	if n, err = io.ReadAtLeast(conn, buf, idDmLen+1); err != nil {
		return
	}
	// check version and cmd
	if buf[idVer] != socksVer5 {
		err = errVer
		return
	}
	if buf[idCmd] != socksCmdConnect {
		err = errCmd
		return
	}

	reqLen := -1
	switch buf[idType] {
	case typeIPv4:
		reqLen = lenIPv4
	case typeIPv6:
		reqLen = lenIPv6
	case typeDm:
		reqLen = int(buf[idDmLen]) + lenDmBase
	default:
		err = errAddrType
		return
	}

	if n == reqLen {
		// common case, do nothing
	} else if n < reqLen { // rare case
		if _, err = io.ReadFull(conn, buf[n:reqLen]); err != nil {
			return
		}
	} else {
		err = errReqExtraData
		return
	}

	rawaddr = buf[idType:reqLen]

	//if debug {
	switch buf[idType] {
	case typeIPv4:
		host = net.IP(buf[idIP0 : idIP0+net.IPv4len]).String()
	case typeIPv6:
		host = net.IP(buf[idIP0 : idIP0+net.IPv6len]).String()
	case typeDm:
		host = string(buf[idDm0 : idDm0+buf[idDmLen]])
	}
	port := binary.BigEndian.Uint16(buf[reqLen-2 : reqLen])
	addr = net.JoinHostPort(host, strconv.Itoa(int(port)))
	//}

	return
}

type ServerCipher struct {
	server string
	cipher *ss.Cipher
}

var servers struct {
	srvCipher []*ServerCipher
	failCnt   []int // failed connection count
}

func parseServerConfig(config *ss.Config) {
	hasPort := func(s string) bool {
		_, port, err := net.SplitHostPort(s)
		if err != nil {
			return false
		}
		return port != ""
	}

	if len(config.ServerPassword) == 0 {
		// only one encryption table
		cipher, err := ss.NewCipher(config.Method, config.Password)
		if err != nil {
			log.Fatal("Failed generating ciphers:", err)
		}
		srvPort := strconv.Itoa(config.ServerPort)
		srvArr := config.GetServerArray()
		n := len(srvArr)
		servers.srvCipher = make([]*ServerCipher, n)

		for i, s := range srvArr {
			if hasPort(s) {
				log.Println("ignore server_port option for server", s)
				servers.srvCipher[i] = &ServerCipher{s, cipher}
			} else {
				servers.srvCipher[i] = &ServerCipher{net.JoinHostPort(s, srvPort), cipher}
			}
		}
	} else {
		// multiple servers
		n := len(config.ServerPassword)
		servers.srvCipher = make([]*ServerCipher, n)

		cipherCache := make(map[string]*ss.Cipher)
		i := 0
		for _, serverInfo := range config.ServerPassword {
			if len(serverInfo) < 2 || len(serverInfo) > 3 {
				log.Fatalf("server %v syntax error\n", serverInfo)
			}
			server := serverInfo[0]
			passwd := serverInfo[1]
			encmethod := ""
			if len(serverInfo) == 3 {
				encmethod = serverInfo[2]
			}
			if !hasPort(server) {
				log.Fatalf("no port for server %s\n", server)
			}
			cipher, ok := cipherCache[passwd]
			if !ok {
				var err error
				cipher, err = ss.NewCipher(encmethod, passwd)
				if err != nil {
					log.Fatal("Failed generating ciphers:", err)
				}
				cipherCache[passwd] = cipher
			}
			servers.srvCipher[i] = &ServerCipher{server, cipher}
			i++
		}
	}
	servers.failCnt = make([]int, len(servers.srvCipher))
	for _, se := range servers.srvCipher {
		log.Println("available remote server", se.server)
	}
	return
}

func connectToServer(serverId int, rawaddr []byte, addr string) (remote *ss.Conn, err error) {
	se := servers.srvCipher[serverId]
	remote, err = ss.DialWithRawAddr(rawaddr, se.server, se.cipher.Copy())
	if err != nil {
		log.Println("error connecting to shadowsocks server:", err)
		const maxFailCnt = 30
		if servers.failCnt[serverId] < maxFailCnt {
			servers.failCnt[serverId]++
		}
		return nil, err
	}
	debug.Printf("connected to %s via %s\n", addr, se.server)
	servers.failCnt[serverId] = 0
	return
}

// Connection to the server in the order specified in the config. On
// connection failure, try the next server. A failed server will be tried with
// some probability according to its fail count, so we can discover recovered
// servers.
func createServerConn(rawaddr []byte, addr string) (remote *ss.Conn, err error) {
	const baseFailCnt = 20
	n := len(servers.srvCipher)
	skipped := make([]int, 0)
	for i := 0; i < n; i++ {
		// skip failed server, but try it with some probability
		if servers.failCnt[i] > 0 && rand.Intn(servers.failCnt[i]+baseFailCnt) != 0 {
			skipped = append(skipped, i)
			continue
		}
		remote, err = connectToServer(i, rawaddr, addr)
		if err == nil {
			return
		}
	}
	// last resort, try skipped servers, not likely to succeed
	for _, i := range skipped {
		remote, err = connectToServer(i, rawaddr, addr)
		if err == nil {
			return
		}
	}
	return nil, err
}

func IsReset(err error) bool {
	if e, ok := err.(*net.OpError); ok && e.Op == "read" {
		if errno, ok := e.Err.(syscall.Errno); ok {
			if errno == syscall.ECONNRESET {
				return true
			}
		}
	}

	return false
}

func NetpollerReadTimeout(err error) bool {
	if e, ok := err.(*net.OpError); ok && e.Op == "read" {
		// not a syscall timeo
		if _, ok := e.Err.(syscall.Errno); !ok {
			return e.Timeout()
		}
	}

	return false
}

// babysitting the direct connection
func directDuplexCopyTimeout(remote net.Conn, conn net.Conn, buf io.Writer) (received int64, err error) {

	var extra int64

	wait := make(chan struct{})

	go func() {
		to := io.MultiWriter(remote, buf)
		io.Copy(to, conn)
		remote.SetReadDeadline(time.Now())
		close(wait)
	}()

	checkSender := func(w <-chan struct{}) bool {
		select {
		case <-w:
			// sender exited
			return false
		default:
			return true
		}
	}

	remote.SetReadDeadline(time.Now().Add(time.Second))
	extra, err = io.Copy(conn, remote)
	received += extra

	if NetpollerReadTimeout(err) {
		// have data received
		if extra > 0 {
			remote.SetReadDeadline(time.Time{})
			// deadline should extended before check sender
			// give sender a chance to end rceiver
			if ok := checkSender(wait); ok {
				extra, err = io.Copy(conn, remote)
				received += extra
			}
		}
	}

	// OpError{Err: net.timeoutError}
	if NetpollerReadTimeout(err) {
		err = nil
	}

	// returning with:
	// 1. nil - remote closed connection
	// 2. other non-netpoller (os/syscall - reset etc.) errors
	// end the sender goroutine
	conn.SetReadDeadline(time.Now())
	<-wait

	// reset conn timer to accept new data
	conn.SetReadDeadline(time.Time{})

	return
}

func directDuplexCopy(remote net.Conn, conn net.Conn, buf io.Writer) (received int64, err error) {

	wait := make(chan struct{})

	go func() {
		to := io.MultiWriter(remote, buf)
		io.Copy(to, conn)
		remote.SetReadDeadline(time.Now())
		close(wait)
	}()

	received, err = io.Copy(conn, remote)

	// OpError{Err: net.timeoutError}
	if NetpollerReadTimeout(err) {
		err = nil
	}

	// end the sender goroutine
	conn.SetReadDeadline(time.Now())
	defer conn.SetReadDeadline(time.Time{})

	<-wait

	// returning with:
	// 1. nil - remote closed connection
	// 2. other non-netpoller (os/syscall - reset etc.) errors

	return
}

func forwardDuplexCopyClose(remote net.Conn, conn net.Conn, buf *bytes.Buffer) (received int64, err error) {
	go func() {
		if buf.Len() > 0 {
			io.Copy(remote, buf)
			//log.Printf("[forward] buffer sent %d (%s)", sent, e)
		}
		io.Copy(remote, conn)
		remote.SetReadDeadline(time.Now())
		//log.Printf("[forward2] buffer sent %d (%s)", sent, e)
	}()

	received, err = io.Copy(conn, remote)
	conn.SetReadDeadline(time.Now())

	//log.Printf("[forward2] buffer got %d (%s)", received, err)

	remote.Close()
	conn.Close()

	if NetpollerReadTimeout(err) {
		err = nil
	}
	return

}

func handleConnection(conn net.Conn) {
	var forked bool
	start := time.Now()
	if debug {
		debug.Printf("socks connect from %s\n", conn.RemoteAddr().String())
	}
	defer func() {
		if !forked {
			conn.Close()
		}
	}()

	var err error = nil
	if err = handShake(conn); err != nil {
		log.Println("socks handshake:", err)
		return
	}
	rawaddr, addr, host, err := getRequest(conn)
	if err != nil {
		log.Println("error getting request:", err)
		return
	}
	// Sending connection established message immediately to client.
	// This some round trip time for creating socks connection with the client.
	// But if connection failed, the client will get connection reset error.
	_, err = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x08, 0x43})
	if err != nil {
		debug.Println("send connection confirmation:", err)
		return
	}

	buf := new(bytes.Buffer)

	var remote net.Conn
	var received int64

	var newSockSite bool
	if site, confirmed := remoteSites.Get(host); site != nil {
		var hint string

		if confirmed {
			hint = "hit"
		} else {
			newSockSite = true
			hint = "-z-"
		}

		remote, err = createServerConn(rawaddr, addr)
		if err != nil {
			if len(servers.srvCipher) > 1 {
				log.Println("Failed connect to all avaiable shadowsocks server")
			}
			return
		}

		log.Printf("[%s] socks5 connected to %s", hint, addr)
		received, err = forwardDuplexCopyClose(remote, conn, buf)
	} else {
		// try direct connect first
		if remote, err = net.DialTimeout("tcp", addr, 1000*time.Millisecond); err == nil {
			log.Printf("-- direct connected to %s", addr)
			received, err = directDuplexCopy(remote, conn, buf)

			switch {
			case received > 0:
				if IsReset(err) {
					if remoteSites.Add(host) {
						log.Printf("[pre] add %s to remote cache - [recv: %d] (%s)", addr, received, err)
					}
				}
				remote.Close()
				conn.Close()
				return
			case received == 0:
				// recoverable connection
				remote.Close()

				if IsReset(err) {
					log.Printf("-- fallthrough to socks5 - %s [req: %d] (err: %s)", addr, buf.Len(), err)
				} else {
					if err != nil {
						log.Printf("-- returning - %s [req: %d] (err: %s)", addr, buf.Len(), err)
					}
					// normal connection end
					conn.Close()
					return
				}

			case received < 0:
				log.Printf("[!!] add %s to remote cache - %d", addr, received)
				remote.Close()
				conn.Close()
				return
			}
		}

		rstart := time.Now()
		slat := rstart.Sub(start).Seconds()

		remote, err = createServerConn(rawaddr, addr)
		if err != nil {
			if len(servers.srvCipher) > 1 {
				log.Println("Failed connect to all avaiable shadowsocks server")
			}
			return
		}

		elat := time.Now().Sub(rstart).Seconds()

		if remoteSites.Add(host) {
			newSockSite = true
			log.Printf("[new] socks5 connected to %s [%.2f+%.2fs]", addr, slat, elat)
		} else {
			log.Printf("[---] socks5 connected to %s [%.2f+%.2fs]", addr, slat, elat)
		}

		received, err = forwardDuplexCopyClose(remote, conn, buf)
	}

	if err != nil {
		log.Printf("[!!!] failed connect to %s - (%s) [%d]", addr, err, received)
	}

	debug.Println("closed connection to", addr)

	if newSockSite {
		if err == nil && received > 0 {
			if remoteSites.Confirm(host) {
				last := time.Now().Sub(start).Seconds()
				log.Printf("[fin] confirmed cache connection to %s [recv: %d bytes - %.2fs]", addr, received, last)
			}
		}
	}
}

func run(listenAddr string) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("starting local socks5 server at %v ...\n", listenAddr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func enoughOptions(config *ss.Config) bool {
	return config.Server != nil && config.ServerPort != 0 &&
		config.LocalPort != 0 && config.Password != ""
}

func main() {
	log.SetOutput(os.Stdout)

	var configFile, cmdServer, cmdLocal string
	var cmdConfig ss.Config
	var printVer bool

	flag.BoolVar(&printVer, "version", false, "print version")
	flag.StringVar(&configFile, "c", "config.json", "specify config file")
	flag.StringVar(&cmdServer, "s", "", "server address")
	flag.StringVar(&cmdLocal, "b", "", "local address, listen only to this address if specified")
	flag.StringVar(&cmdConfig.Password, "k", "", "password")
	flag.IntVar(&cmdConfig.ServerPort, "p", 0, "server port")
	flag.IntVar(&cmdConfig.LocalPort, "l", 0, "local socks5 proxy port")
	flag.StringVar(&cmdConfig.Method, "m", "", "encryption method, default: aes-256-cfb")
	flag.BoolVar((*bool)(&debug), "d", false, "print debug message")

	flag.Parse()

	if printVer {
		ss.PrintVersion()
		os.Exit(0)
	}

	cmdConfig.Server = cmdServer
	ss.SetDebug(debug)

	exists, err := ss.IsFileExists(configFile)
	// If no config file in current directory, try search it in the binary directory
	// Note there's no portable way to detect the binary directory.
	binDir := path.Dir(os.Args[0])
	if (!exists || err != nil) && binDir != "" && binDir != "." {
		oldConfig := configFile
		configFile = path.Join(binDir, "config.json")
		log.Printf("%s not found, try config file %s\n", oldConfig, configFile)
	}

	config, err := ss.ParseConfig(configFile)
	if err != nil {
		config = &cmdConfig
		if !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "error reading %s: %v\n", configFile, err)
			os.Exit(1)
		}
	} else {
		ss.UpdateConfig(config, &cmdConfig)
	}
	if config.Method == "" {
		config.Method = "aes-256-cfb"
	}
	if len(config.ServerPassword) == 0 {
		if !enoughOptions(config) {
			fmt.Fprintln(os.Stderr, "must specify server address, password and both server/local port")
			os.Exit(1)
		}
	} else {
		if config.Password != "" || config.ServerPort != 0 || config.GetServerArray() != nil {
			fmt.Fprintln(os.Stderr, "given server_password, ignore server, server_port and password option:", config)
		}
		if config.LocalPort == 0 {
			fmt.Fprintln(os.Stderr, "must specify local port")
			os.Exit(1)
		}
	}

	parseServerConfig(config)

	run(cmdLocal + ":" + strconv.Itoa(config.LocalPort))
}
