use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn respond_to_prelogin(listener: &TcpListener, response: &[u8]) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut header = [0; 8];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], 0x12, "the client must send PRELOGIN first");
    let length = usize::from(u16::from_be_bytes([header[2], header[3]]));
    assert!((8..4096).contains(&length));
    let mut payload = vec![0; length - 8];
    stream.read_exact(&mut payload).await.unwrap();
    stream.write_all(response).await.unwrap();
    stream
}
