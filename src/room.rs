use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_lite::future::block_on;
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::{Connection, SqliteConnection};
use subtle::ConstantTimeEq;

const SERVER_NAME: &str = "codex-room.local";
const INVITE_PREFIX: &str = "cdp-room-1.";
const INVITE_LIFETIME: Duration = Duration::from_secs(30 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_FRAME: usize = 512 * 1024;
const MAX_MESSAGE_BYTES: usize = 8 * 1024;

pub type Secret = [u8; 32];
type MessageId = [u8; 16];

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Member {
    Host,
    Guest,
}

impl Member {
    pub fn label(self) -> &'static str {
        match self {
            Self::Host => "主协调端",
            Self::Guest => "接入端",
        }
    }

    fn number(self) -> i64 {
        match self {
            Self::Host => 0,
            Self::Guest => 1,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RoomMessage {
    pub id: i64,
    pub author: Member,
    pub body: String,
    pub time: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Outgoing {
    id: MessageId,
    body: String,
}

impl Outgoing {
    pub fn new(body: String) -> io::Result<Self> {
        if body.trim().is_empty() {
            return Err(problem("请输入消息后再发送"));
        }
        if body.len() > MAX_MESSAGE_BYTES {
            return Err(problem("单条消息最多为 8 KiB，请分段发送"));
        }
        Ok(Self {
            id: random()?,
            body,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Invite {
    address: SocketAddrV4,
    certificate: Vec<u8>,
    token: Secret,
}

impl Invite {
    fn encode(&self) -> io::Result<String> {
        let bytes = serde_json::to_vec(self).map_err(other)?;
        Ok(format!("{INVITE_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
    }

    fn decode(code: &str) -> io::Result<Self> {
        let code = code.trim();
        let encoded = code
            .strip_prefix(INVITE_PREFIX)
            .filter(|code| code.len() <= 8192)
            .ok_or_else(|| problem("邀请内容不完整或版本不匹配"))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| problem("邀请内容损坏"))?;
        let invite: Self = serde_json::from_slice(&bytes).map_err(|_| problem("邀请内容损坏"))?;
        if !allowed_address(*invite.address.ip()) || invite.address.port() == 0 {
            return Err(problem("邀请中的地址须为有效的内网 IPv4 地址"));
        }
        Ok(invite)
    }
}

#[derive(Serialize, Deserialize)]
enum Request {
    Join {
        invitation: Secret,
        member: Secret,
    },
    Exchange {
        member: Secret,
        after: i64,
        outgoing: Option<Outgoing>,
        check: Option<MessageId>,
    },
}

#[derive(Serialize, Deserialize)]
enum Reply {
    Joined,
    Update(RoomUpdate),
    Rejected(String),
}

#[derive(Serialize, Deserialize)]
pub struct RoomUpdate {
    pub guest_joined: bool,
    pub messages: Vec<RoomMessage>,
    pub confirmed: bool,
}

#[derive(Clone)]
pub struct RoomClient {
    address: SocketAddrV4,
    tls: Arc<ClientConfig>,
    member: Secret,
}

impl RoomClient {
    fn new(invite: &Invite, member: Secret) -> io::Result<Self> {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(invite.certificate.clone()))
            .map_err(|_| problem("邀请中的主机证书无效"))?;
        let tls =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(other)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        Ok(Self {
            address: invite.address,
            tls: Arc::new(tls),
            member,
        })
    }

    pub fn join(code: &str, member: Secret) -> io::Result<Self> {
        let invite = Invite::decode(code)?;
        let client = Self::new(&invite, member)?;
        match client.request(Request::Join {
            invitation: invite.token,
            member,
        })? {
            Reply::Joined => Ok(client),
            _ => Err(problem("主机返回了无法识别的加入回执")),
        }
    }

    pub fn exchange(
        &self,
        after: i64,
        outgoing: Option<Outgoing>,
        pending: Option<&Outgoing>,
    ) -> io::Result<RoomUpdate> {
        let check = pending.or(outgoing.as_ref()).map(|message| message.id);
        let request = Request::Exchange {
            member: self.member,
            after,
            outgoing,
            check,
        };
        match self.request(request)? {
            Reply::Update(update) => Ok(update),
            _ => Err(problem("主机返回了无法识别的消息回执")),
        }
    }

    fn request(&self, request: Request) -> io::Result<Reply> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let socket = TcpStream::connect_timeout(&self.address.into(), REQUEST_TIMEOUT)?;
        let name = ServerName::try_from(SERVER_NAME).map_err(other)?;
        let conn = ClientConnection::new(self.tls.clone(), name).map_err(other)?;
        let mut stream = StreamOwned::new(conn, TimedSocket { socket, deadline });
        write_frame(&mut stream, &request)?;
        match read_frame(&mut stream)? {
            Reply::Rejected(message) => Err(problem(&message)),
            reply => Ok(reply),
        }
    }
}

pub struct RoomHost {
    stop: Arc<AtomicBool>,
}

impl Drop for RoomHost {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl RoomHost {
    pub fn start(ip: Ipv4Addr, directory: &Path) -> io::Result<(Self, RoomClient, String)> {
        if !allowed_address(ip) {
            return Err(problem("请选择本机的内网 IPv4 地址"));
        }
        let listener = TcpListener::bind((ip, 0))?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()]).map_err(other)?;
        let certificate = cert.der().clone();
        let tls =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(other)?
                .with_no_client_auth()
                .with_single_cert(
                    vec![certificate.clone()],
                    PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into(),
                )
                .map_err(other)?;
        let invite = Invite {
            address: SocketAddrV4::new(ip, port),
            certificate: certificate.as_ref().to_vec(),
            token: random()?,
        };
        let owner = random()?;
        let client = RoomClient::new(&invite, owner)?;
        let code = invite.encode()?;
        std::fs::create_dir_all(directory)?;
        let room_id = URL_SAFE_NO_PAD.encode(random::<16>()?);
        let path = directory.join(format!("{room_id}.sqlite3"));
        let store = block_on(open_room(&path)).map_err(other)?;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let mut room = ServerRoom {
            store,
            owner,
            invitation: invite.token,
            guest: None,
            started: Instant::now(),
        };
        let tls = Arc::new(tls);
        std::thread::Builder::new()
            .name("lan-room".into())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((socket, _)) => {
                            let _ = room.serve(socket, tls.clone(), &flag);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        Err(_) => break,
                    }
                }
            })?;
        Ok((Self { stop }, client, code))
    }
}

struct ServerRoom {
    store: SqliteConnection,
    owner: Secret,
    invitation: Secret,
    guest: Option<Secret>,
    started: Instant,
}

impl ServerRoom {
    fn serve(
        &mut self,
        socket: TcpStream,
        tls: Arc<ServerConfig>,
        stop: &AtomicBool,
    ) -> io::Result<()> {
        socket.set_nonblocking(false)?;
        let conn = ServerConnection::new(tls).map_err(other)?;
        let mut stream = StreamOwned::new(
            conn,
            TimedSocket {
                socket,
                deadline: Instant::now() + REQUEST_TIMEOUT,
            },
        );
        let request = read_frame(&mut stream)?;
        if stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        write_frame(&mut stream, &self.handle(request))
    }

    fn handle(&mut self, request: Request) -> Reply {
        match request {
            Request::Join { invitation, member } => {
                if !bool::from(self.invitation.ct_eq(&invitation)) {
                    return Reply::Rejected("邀请无效".into());
                }
                if let Some(guest) = self.guest {
                    return if bool::from(guest.ct_eq(&member)) {
                        Reply::Joined
                    } else {
                        Reply::Rejected("该邀请已被使用".into())
                    };
                }
                if self.started.elapsed() >= INVITE_LIFETIME {
                    return Reply::Rejected("邀请已过期，请由主协调端重新创建房间".into());
                }
                self.guest = Some(member);
                Reply::Joined
            }
            Request::Exchange {
                member,
                after,
                outgoing,
                check,
            } => {
                let role = if bool::from(self.owner.ct_eq(&member)) {
                    Member::Host
                } else if self
                    .guest
                    .is_some_and(|guest| bool::from(guest.ct_eq(&member)))
                {
                    Member::Guest
                } else {
                    return Reply::Rejected("尚未加入该房间".into());
                };
                if after < 0
                    || outgoing.as_ref().is_some_and(|msg| {
                        msg.body.trim().is_empty() || msg.body.len() > MAX_MESSAGE_BYTES
                    })
                {
                    return Reply::Rejected("消息内容或读取位置不合法".into());
                }
                match block_on(exchange_room(&mut self.store, role, after, outgoing, check)) {
                    Ok((messages, confirmed)) => Reply::Update(RoomUpdate {
                        messages,
                        confirmed,
                        guest_joined: self.guest.is_some(),
                    }),
                    Err(_) => Reply::Rejected("主机暂时无法保存或读取房间消息".into()),
                }
            }
        }
    }
}

async fn open_room(path: &Path) -> sqlx::Result<SqliteConnection> {
    let mut store = crate::storage::connect(path).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_messages (
            id INTEGER PRIMARY KEY,
            author INTEGER NOT NULL CHECK (author IN (0, 1)),
            request_id BLOB NOT NULL,
            body TEXT NOT NULL,
            sent_at INTEGER NOT NULL DEFAULT (unixepoch()),
            UNIQUE(author, request_id)
        )",
    )
    .execute(&mut store)
    .await?;
    Ok(store)
}

async fn exchange_room(
    store: &mut SqliteConnection,
    role: Member,
    after: i64,
    outgoing: Option<Outgoing>,
    check: Option<MessageId>,
) -> sqlx::Result<(Vec<RoomMessage>, bool)> {
    let mut transaction = store.begin().await?;
    if let Some(outgoing) = outgoing {
        sqlx::query(
            "INSERT INTO room_messages (author, request_id, body) VALUES (?1, ?2, ?3)
             ON CONFLICT(author, request_id) DO NOTHING",
        )
        .bind(role.number())
        .bind(outgoing.id.as_slice())
        .bind(outgoing.body)
        .execute(&mut *transaction)
        .await?;
    }
    let confirmed = if let Some(id) = check {
        sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS(SELECT 1 FROM room_messages WHERE author = ?1 AND request_id = ?2)",
        )
        .bind(role.number())
        .bind(id.as_slice())
        .fetch_one(&mut *transaction)
        .await?
            != 0
    } else {
        false
    };
    let rows: Vec<(i64, i64, String, String)> = sqlx::query_as(
        "SELECT id, author, body, strftime('%Y-%m-%d %H:%M', sent_at, 'unixepoch', 'localtime')
         FROM room_messages WHERE id > ?1 ORDER BY id LIMIT 8",
    )
    .bind(after)
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let messages = rows
        .into_iter()
        .map(|(id, author, body, time)| RoomMessage {
            id,
            author: if author == 0 {
                Member::Host
            } else {
                Member::Guest
            },
            body,
            time,
        })
        .collect();
    Ok((messages, confirmed))
}

pub fn private_addresses() -> io::Result<Vec<Ipv4Addr>> {
    let mut addresses: Vec<_> = if_addrs::get_if_addrs()?
        .into_iter()
        .filter_map(|interface| match interface.ip() {
            std::net::IpAddr::V4(ip) if allowed_address(ip) && !ip.is_loopback() => Some(ip),
            _ => None,
        })
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

pub fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0u8; N];
    getrandom::fill(&mut bytes).map_err(|_| io::Error::other("无法生成房间随机标识"))?;
    Ok(bytes)
}

fn allowed_address(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_loopback() || ip.is_link_local()
}

fn problem(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn other(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(error)
}

fn read_frame<T: DeserializeOwned>(stream: &mut impl Read) -> io::Result<T> {
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        return Err(problem("消息帧超出允许大小"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).map_err(other)
}

fn write_frame<T: Serialize>(stream: &mut impl Write, value: &T) -> io::Result<()> {
    let encoded = serde_json::to_vec(value).map_err(other)?;
    if encoded.len() > MAX_FRAME {
        return Err(problem("消息帧超出允许大小"));
    }
    stream.write_all(&(encoded.len() as u32).to_be_bytes())?;
    stream.write_all(&encoded)?;
    stream.flush()
}

struct TimedSocket {
    socket: TcpStream,
    deadline: Instant,
}

impl TimedSocket {
    fn timeout(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|left| !left.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "房间连接超时"))
    }
}

impl Read for TimedSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.socket.set_read_timeout(Some(self.timeout()?))?;
        self.socket.read(buffer)
    }
}

impl Write for TimedSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.socket.set_write_timeout(Some(self.timeout()?))?;
        self.socket.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_members_exchange_and_store_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let (_host, owner, code) = RoomHost::start(Ipv4Addr::LOCALHOST, directory.path()).unwrap();
        let guest_identity = random().unwrap();
        let guest = RoomClient::join(&code, guest_identity).unwrap();
        assert!(RoomClient::join(&code, random().unwrap()).is_err());
        assert!(RoomClient::join(&code, guest_identity).is_ok());

        let first = Outgoing::new("主协调端\n中文测试".into()).unwrap();
        assert!(
            owner
                .exchange(0, Some(first.clone()), None)
                .unwrap()
                .confirmed
        );
        assert!(owner.exchange(0, Some(first), None).unwrap().confirmed);
        let view = guest.exchange(0, None, None).unwrap();
        assert_eq!(view.messages.len(), 1);
        assert_eq!(view.messages[0].author, Member::Host);
        assert_eq!(view.messages[0].body, "主协调端\n中文测试");
        let after = view.messages[0].id;
        let response = Outgoing::new("接入端回报".into()).unwrap();
        guest.exchange(after, Some(response.clone()), None).unwrap();
        assert!(
            guest
                .exchange(after, None, Some(&response))
                .unwrap()
                .confirmed
        );
        let incoming = owner.exchange(after, None, None).unwrap();
        assert_eq!(incoming.messages.len(), 1);
        assert_eq!(incoming.messages[0].author, Member::Guest);
        assert_eq!(incoming.messages[0].body, "接入端回报");

        let stored = std::fs::read_dir(directory.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        block_on(async {
            let mut store = open_room(&stored).await.unwrap();
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM room_messages")
                .fetch_one(&mut store)
                .await
                .unwrap();
            assert_eq!(count, 2);
        });
    }

    #[test]
    fn rejects_wrong_certificate_invite_and_member() {
        let directory = tempfile::tempdir().unwrap();
        let a = directory.path().join("a");
        let b = directory.path().join("b");
        let (_host, _, code) = RoomHost::start(Ipv4Addr::LOCALHOST, &a).unwrap();
        let (_other, _, other_code) = RoomHost::start(Ipv4Addr::LOCALHOST, &b).unwrap();
        let mut invite = Invite::decode(&code).unwrap();
        let wrong = RoomClient::new(&invite, random().unwrap()).unwrap();
        assert!(wrong.exchange(0, None, None).is_err());
        invite.token = random().unwrap();
        assert!(RoomClient::join(&invite.encode().unwrap(), random().unwrap()).is_err());
        invite.certificate = Invite::decode(&other_code).unwrap().certificate;
        assert!(RoomClient::join(&invite.encode().unwrap(), random().unwrap()).is_err());
    }
}
