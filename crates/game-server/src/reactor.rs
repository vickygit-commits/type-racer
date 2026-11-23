
use std::io;
use std::os::fd::RawFd;

pub struct Reactor {
    epoll_fd: RawFd,
}

impl Reactor {
    pub fn new() -> io::Result<Self> {
        let epoll_fd = unsafe {
            libc::epoll_create1(0)
        };

        if epoll_fd == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { epoll_fd })
    }

    pub fn register(&self, fd: RawFd) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: libc::EPOLLIN as u32, //To make the connection is ready to accept from listening and incoming data is available for client socket
            u64: fd as u64,
        };

        let result = unsafe {
            libc::epoll_ctl(
                self.epoll_fd,
                libc::EPOLL_CTL_ADD,
                fd,
                &mut event,
            )
        };

        if result == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn wait(
        &self,
        events: &mut [libc::epoll_event],
    ) -> io::Result<usize> {
        let count = unsafe {
            libc::epoll_wait(
                self.epoll_fd,
                events.as_mut_ptr(),
                events.len() as i32,
                -1,
            )
        };

        if count == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(count as usize)
    }

    pub fn modify(&self, fd: RawFd, flags: u32) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: flags,
            u64: fd as u64,
        };

        let result = unsafe {
            libc::epoll_ctl(
                self.epoll_fd,
                libc::EPOLL_CTL_MOD,
                fd,
                &mut event,
            )
        };
        if result == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

    pub fn unregister(&self, fd: RawFd) -> io::Result<()> {
        let result = unsafe {
            libc::epoll_ctl(
                self.epoll_fd,
                libc::EPOLL_CTL_DEL,
                fd,
                std::ptr::null_mut(),
            )
        };

        if result == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(())
    }

}

impl Drop for Reactor {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.epoll_fd);
        }
    }
}
