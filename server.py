from gevent.server import StreamServer
import os


def mangodb(socket, address):
    if os.environ.get('MANGODB_USE_BCRYPT', False):
        import bcrypt
    else:
        bcrypt = None

    socket.sendall('HELLO\r\n')
    client = socket.makefile()
    output = open('/dev/null', 'w')
    while 1:
        line = client.readline()
        if not line:
            break
        cmd_bits = line.split(' ', 1)
        cmd = cmd_bits[0]
        if cmd == 'BYE':
            break
        if len(cmd_bits) > 1:
            output.write(cmd_bits[1])
            if os.environ.get('MANGODB_DURABLE', False):
                output.flush()
                os.fsync(output.fileno())
            data = os.urandom(1024)
            if os.environ.get('MANGODB_USE_BCRYPT', True):
                data = bcrypt.hashpw(data.encode('string-escape'), bcrypt.gensalt())
            client.write('OK' + data.encode('string-escape') + '\r\n')
        client.flush()


if __name__ == '__main__':
    server = StreamServer(('0.0.0.0', 27017), mangodb)
    print ('Starting MangoDB on port 27017')
    server.serve_forever()
