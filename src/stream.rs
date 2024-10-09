use std::{fmt::Debug, marker::PhantomData};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::anyhow;
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{Decoder, Encoder};

use quinn::{RecvStream, SendStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug)]
pub struct P2pQuicStream {
    read: RecvStream,
    write: SendStream,
}

impl PartialEq for P2pQuicStream {
    fn eq(&self, other: &Self) -> bool {
        self.read.id() == other.read.id() && self.write.id() == other.write.id()
    }
}

impl Eq for P2pQuicStream {}

impl P2pQuicStream {
    pub fn new(read: RecvStream, write: SendStream) -> Self {
        Self { read, write }
    }
}

impl AsyncRead for P2pQuicStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().read).poll_read(cx, buf)
    }
}

impl AsyncWrite for P2pQuicStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
        let w: &mut (dyn AsyncWrite + Unpin) = &mut self.get_mut().write;
        Pin::new(w).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().write).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.get_mut().write).poll_shutdown(cx)
    }
}

pub struct BincodeCodec<Item> {
    length_decode: LengthDelimitedCodec,
    _tmp: PhantomData<Item>,
}

impl<Item> Default for BincodeCodec<Item> {
    fn default() -> Self {
        Self {
            length_decode: LengthDelimitedCodec::default(),
            _tmp: Default::default(),
        }
    }
}

impl<Item: Serialize> Encoder<Item> for BincodeCodec<Item> {
    type Error = std::io::Error;

    fn encode(&mut self, item: Item, dst: &mut tokio_util::bytes::BytesMut) -> Result<(), Self::Error> {
        let data: Vec<u8> = bincode::serialize(&item).map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bincode serialize failure"))?;
        self.length_decode.encode(data.into(), dst)
    }
}

impl<Item: DeserializeOwned + Debug> Decoder for BincodeCodec<Item> {
    type Error = std::io::Error;
    type Item = Item;

    fn decode(&mut self, src: &mut tokio_util::bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.length_decode.decode(src)? {
            Some(buf) => Ok(Some(
                bincode::deserialize(&buf).map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bincode deserialize failure"))?,
            )),
            None => Ok(None),
        }
    }
}

pub async fn wait_object<R: AsyncRead + Unpin, O: DeserializeOwned, const MAX_SIZE: usize>(reader: &mut R) -> anyhow::Result<O> {
    let mut len_buf = [0; 2];
    let mut data_buf = [0; MAX_SIZE];
    reader.read_exact(&mut len_buf).await?;
    let handshake_len = u16::from_be_bytes([len_buf[0], len_buf[1]]) as usize;
    if handshake_len > data_buf.len() {
        return Err(anyhow!("packet to big {} vs {MAX_SIZE}", data_buf.len()));
    }

    reader.read_exact(&mut data_buf[0..handshake_len]).await?;

    Ok(bincode::deserialize(&data_buf[0..handshake_len])?)
}

pub async fn write_object<W: AsyncWrite + Send + Unpin, O: Serialize, const MAX_SIZE: usize>(writer: &mut W, object: &O) -> anyhow::Result<()> {
    let data_buf: Vec<u8> = bincode::serialize(&object).expect("Should convert to binary");
    if data_buf.len() > MAX_SIZE {
        return Err(anyhow!("buffer to big {} vs {MAX_SIZE}", data_buf.len()));
    }
    let len_buf = (data_buf.len() as u16).to_be_bytes();

    writer.write_all(&len_buf).await?;
    writer.write_all(&data_buf).await?;
    Ok(())
}
