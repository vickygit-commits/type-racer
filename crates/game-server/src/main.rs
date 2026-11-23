mod reactor;
mod server;
use std::io;
fn main() -> io::Result<()> {
//     let listener = TcpListener::bind("127.0.0.1:8080")?;

//     listener.set_nonblocking(true)?;
//     println!("listening on 127.0.0.1:8080");
//     let mut clients: Vec<Client> = Vec::new();
//     // for stream in listener.incoming() {
//     //     let mut stream = stream?;
//     //     let peer = stream.peer_addr()?;
//     //     println!("New connection: {peer}");

//     //     let mut buf = [0u8; 1024];
//     //     loop {
//     //         let n = stream.read(&mut buf)?;
//     //         if n == 0 {
//     //             break;
//     //         }
//     //         stream.write_all(&buf[..n])?;
//     //     }
//     // }
 
//     // Ok(())

//     loop {
//         loop {
//             match listener.accept() {
//                 Ok((stream,addr)) => {
//                     stream.set_nonblocking(true)?;
//                     println!("New connection: {addr}");
//                     clients.push(Client {
//                         stream,
//                         outgoing: Vec::new(),
//                         written: 0,
//                     });
//                 }
//                 Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
//                 break;
//             }
//              Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
//                 continue;
//             }
//                 Err(e) => return Err(e)

//             }
// }
//             clients.retain_mut(|client|{
//                 let mut disconnected = false;
//                 let mut buf = [0u8; 1024];

//                 loop {
//                     match client.stream.read(&mut buf) {
//                         Ok(0) => {
//                             println!("EOF received from client");
//                             disconnected = true;
//                             break;
//                         }
//                         Ok(n) => {
//                             println!("Received {n} bytes");
//                             client.outgoing.extend_from_slice(&buf[..n]);
//                         }
//                         Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
//                             break;
//                         }
//                         Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
//                             continue;
//                         }
//                         Err(_) => {
//                             disconnected = true;
//                             break;
//                         }
//                     }

//                 }
//                 if disconnected {
//                     return false;
//                 }

//                 while client.written < client.outgoing.len() {
//                     match client.stream.write(
//                         &client.outgoing[client.written ..]
//                     ) {
//                         Ok(0) => {
//                             println!("EOF received from client");
//                             disconnected = true;
//                             break;
//                         }
//                         Ok(n) => {
//                             println!("Wriiten {n} bytes");
//                             client.written +=n;
//                         }

//                         Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
//                             break;
//                         }

//                         Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
//                             continue;
//                         }
//                         Err(_) => {
//                             disconnected = true;
//                             break;
//                         }
//                     }
//                 }

//                 if disconnected {
//                     return false;
//                 }
//                 if client.written == client.outgoing.len() {
//                     client.outgoing.clear();
//                     client.written = 0;
//                 }
//                 true
                
//             });
//             // thread::sleep(Duration::from_millis(10));

//     }

    let reactor = reactor::Reactor::new()?;
    let mut server = server::Server::new("127.0.0.1:8080")?;
    server.register(&reactor)?;

    println!("Server listening on 127.0.0.1:8080");
    let mut events: [libc::epoll_event; 10] = unsafe {std::mem::zeroed() };
    loop {
        let count = match reactor.wait(&mut events) {
            Ok(count) => count,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {
                continue;
            }
            Err(e) => return Err(e),
        };

        for i in 0..count {
            let ready_fd = events[i].u64 as i32;
            let flags = events[i].events;
            if ready_fd == server.listener_fd() {
                server.accept_connections(&reactor)?;
            }
            else {
                server.handle_client_event(ready_fd, flags, &reactor)?;
            }
        }
    }
}