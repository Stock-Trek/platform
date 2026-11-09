use std::{collections::HashMap, io, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

pub(crate) struct FakeValkey;

impl FakeValkey {
    pub(crate) async fn start() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let address = listener.local_addr().expect("server address");
        let state = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = Self::serve(stream, state).await;
                });
            }
        });
        format!("redis://{address}")
    }

    async fn serve(
        stream: TcpStream,
        state: Arc<Mutex<HashMap<String, String>>>,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(stream);
        while let Some(command) = Self::read_command(&mut reader).await? {
            let response = Self::execute(&command, &state).await;
            reader.get_mut().write_all(&response).await?;
        }
        Ok(())
    }

    async fn read_command(reader: &mut BufReader<TcpStream>) -> io::Result<Option<Vec<Vec<u8>>>> {
        let Some(line) = Self::read_line(reader).await? else {
            return Ok(None);
        };
        if line.first() != Some(&b'*') {
            return Err(io::Error::other("expected a RESP array"));
        }
        let count = Self::parse_number(&line[1..])?;
        let mut command = Vec::with_capacity(count);
        for _ in 0..count {
            let length_line = Self::read_line(reader)
                .await?
                .ok_or_else(|| io::Error::other("missing bulk string length"))?;
            if length_line.first() != Some(&b'$') {
                return Err(io::Error::other("expected a RESP bulk string"));
            }
            let length = Self::parse_number(&length_line[1..])?;
            let mut value = vec![0; length + 2];
            reader.read_exact(&mut value).await?;
            value.truncate(length);
            command.push(value);
        }
        Ok(Some(command))
    }

    async fn read_line(reader: &mut BufReader<TcpStream>) -> io::Result<Option<Vec<u8>>> {
        let mut line = Vec::new();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Ok(None);
        }
        while matches!(line.last(), Some(b'\r' | b'\n')) {
            line.pop();
        }
        Ok(Some(line))
    }

    fn parse_number(value: &[u8]) -> io::Result<usize> {
        std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| io::Error::other("invalid RESP number"))
    }

    async fn execute(command: &[Vec<u8>], state: &Mutex<HashMap<String, String>>) -> Vec<u8> {
        let name = String::from_utf8_lossy(&command[0]).to_ascii_uppercase();
        match name.as_str() {
            "CLIENT" | "SELECT" => b"+OK\r\n".to_vec(),
            "PING" => b"+PONG\r\n".to_vec(),
            "SET" => Self::set(command, state).await,
            "EVAL" | "EVALSHA" => Self::release(command, state).await,
            _ => b"-ERR unsupported command\r\n".to_vec(),
        }
    }

    async fn set(command: &[Vec<u8>], state: &Mutex<HashMap<String, String>>) -> Vec<u8> {
        let key = String::from_utf8_lossy(&command[1]).into_owned();
        let value = String::from_utf8_lossy(&command[2]).into_owned();
        let nx = command
            .iter()
            .any(|argument| argument.eq_ignore_ascii_case(b"NX"));
        let mut state = state.lock().await;
        if nx && state.contains_key(&key) {
            return b"$-1\r\n".to_vec();
        }
        state.insert(key, value);
        b"+OK\r\n".to_vec()
    }

    async fn release(command: &[Vec<u8>], state: &Mutex<HashMap<String, String>>) -> Vec<u8> {
        let key_count = Self::parse_number(&command[2]).unwrap_or(1);
        let key = String::from_utf8_lossy(&command[3]).into_owned();
        let value = String::from_utf8_lossy(&command[3 + key_count]).into_owned();
        let mut state = state.lock().await;
        if state.get(&key) == Some(&value) {
            state.remove(&key);
            b":1\r\n".to_vec()
        } else {
            b":0\r\n".to_vec()
        }
    }
}
