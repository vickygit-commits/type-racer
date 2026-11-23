
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;

use crate::reactor::Reactor;

struct Client {
    stream: TcpStream,
    outgoing: Vec<u8>,
    written: usize,
}

pub struct Server {
    listener: TcpListener,
    clients: HashMap<i32, Client>,
}

impl Server {
    pub fn new(address: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;

        Ok(Self {
            listener,
            clients: HashMap::new(),
        })
    }

    pub fn register(&self, reactor: &Reactor) -> io::Result<()> {
        reactor.register(self.listener.as_raw_fd())
    }

    pub fn listener_fd(&self) -> i32 {
        self.listener.as_raw_fd()
    }

    pub fn accept_connections(
        &mut self,
        reactor: &Reactor,
    ) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    stream.set_nonblocking(true)?;

                    let client_fd = stream.as_raw_fd();

                    // Register the client socket for incoming data.
                    reactor.register(client_fd)?;

                    self.clients.insert(
                        client_fd,
                        Client {
                            stream,
                            outgoing: Vec::new(),
                            written: 0,
                        },
                    );

                    println!("New connection: {addr}, FD: {client_fd}");
                }

                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }

                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }

                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    pub fn handle_client_event(
        &mut self,
        fd: i32,
        flags: u32,
        reactor: &Reactor,
    ) -> io::Result<()> {
        let mut disconnected = false;

        if let Some(client) = self.clients.get_mut(&fd) {
            // Handle incoming data.
            if flags & libc::EPOLLIN as u32 != 0 {
                let mut buffer = [0u8; 1024];

                loop {
                    match client.stream.read(&mut buffer) {
                        Ok(0) => {
                            println!("Client EOF: FD {fd}");
                            disconnected = true;
                            break;
                        }

                        Ok(n) => {
                            println!("Received {n} bytes from FD {fd}");
                            client.outgoing.extend_from_slice(&buffer[..n]);
                        }

                        Err(ref e)
                            if e.kind() == io::ErrorKind::WouldBlock =>
                        {
                            break;
                        }

                        Err(ref e)
                            if e.kind() == io::ErrorKind::Interrupted =>
                        {
                            continue;
                        }

                        Err(e) => {
                            eprintln!("Read error on FD {fd}: {e}");
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            if !disconnected {
                while client.written < client.outgoing.len() {
                    match client.stream.write(
                        &client.outgoing[client.written..],
                    ) {
                        Ok(0) => {
                            disconnected = true;
                            break;
                        }

                        Ok(n) => {
                            client.written += n;
                            println!("Written {n} bytes to FD {fd}");
                        }

                        Err(ref e)
                            if e.kind() == io::ErrorKind::WouldBlock =>
                        {
                            break;
                        }

                        Err(ref e)
                            if e.kind() == io::ErrorKind::Interrupted =>
                        {
                            continue;
                        }

                        Err(e) => {
                            eprintln!("Write error on FD {fd}: {e}");
                            disconnected = true;
                            break;
                        }
                    }
                }
            }

            // Clear the buffer after all bytes have been written.
            if !disconnected && client.written == client.outgoing.len() {
                client.outgoing.clear();
                client.written = 0;
            }
        }

        if disconnected {
            reactor.unregister(fd)?;
            self.clients.remove(&fd);

            println!("Removed client FD: {fd}");
            return Ok(());
        }

        // Watch for EPOLLOUT only when there is pending data.
        if let Some(client) = self.clients.get(&fd) {
            let mut interests = libc::EPOLLIN as u32;

            if client.written < client.outgoing.len() {
                interests |= libc::EPOLLOUT as u32;
            }

            reactor.modify(fd, interests)?;
        }

        Ok(())
    }
}
