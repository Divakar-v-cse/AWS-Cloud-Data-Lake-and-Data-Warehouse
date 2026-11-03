"""Find a port that can actually be bound (Get-NetTCPConnection missed a live
listener on 8010, so probe by binding instead of by inspection)."""
import socket

for port in (8000, 8010, 8020, 8030, 8040, 8050, 8100):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", port))
        s.listen(1)
        print(f"{port} FREE")
    except OSError as exc:
        print(f"{port} BUSY ({exc.errno})")
    finally:
        s.close()
