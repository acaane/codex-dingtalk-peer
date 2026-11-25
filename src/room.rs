use std::{
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_lite::future::block_on;
use ring::digest::{SHA256, digest};
use rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::{Connection, SqliteConnection};
use subtle::ConstantTimeEq;

use crate::readiness;
use crate::room_sessions::Sessions;
use crate::tasks;

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

#[derive(Clone, Serialize, Deserialize)]
struct Invite {
    address: SocketAddrV4,
    certificate: Vec<u8>,
    token: Secret,
}

impl Invite {
    fn room_id(&self) -> String {
        URL_SAFE_NO_PAD.encode(digest(&SHA256, &self.certificate))
    }

    fn encode(&self) -> io::Result<String> {
        let bytes = serde_json::to_vec(self).map_err(other)?;
        Ok(format!("{INVITE_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
    }

    fn decode(code: &str) -> io::Result<Self> {
        let code = code.trim();
        if code.is_empty() {
            return Err(problem("请输入完整邀请"));
        }
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        readiness: Option<readiness::Request>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task: Option<tasks::Request>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readiness: Option<readiness::Update>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<tasks::Update>,
}

#[derive(Clone)]
pub struct RoomClient {
    address: SocketAddrV4,
    tls: Arc<ClientConfig>,
    member: Secret,
}

impl RoomClient {
    pub fn for_saved_host(directory: &Path, room_id: &str) -> io::Result<Self> {
        match Sessions::open_existing(directory)?.get::<SavedRoom>(room_id)? {
            Some(SavedRoom::Host { invite, owner, .. }) => Self::new(&invite, owner),
            _ => Err(problem("本机没有该房间的主协调端身份")),
        }
    }

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

    fn join(invite: &Invite, member: Secret) -> io::Result<Self> {
        let client = Self::new(invite, member)?;
        match client.request(Request::Join {
            invitation: invite.token,
            member,
        })? {
            Reply::Joined => Ok(client),
            _ => Err(problem("主机返回了无法识别的加入回执")),
        }
    }

    #[cfg(test)]
    pub fn exchange(
        &self,
        after: i64,
        outgoing: Option<Outgoing>,
        pending: Option<&Outgoing>,
    ) -> io::Result<RoomUpdate> {
        self.exchange_with_readiness(after, outgoing, pending, None)
    }

    #[cfg(test)]
    pub fn exchange_with_readiness(
        &self,
        after: i64,
        outgoing: Option<Outgoing>,
        pending: Option<&Outgoing>,
        readiness: Option<readiness::Request>,
    ) -> io::Result<RoomUpdate> {
        self.exchange_room(after, outgoing, pending, readiness, None)
    }

    pub fn exchange_room(
        &self,
        after: i64,
        outgoing: Option<Outgoing>,
        pending: Option<&Outgoing>,
        readiness: Option<readiness::Request>,
        task: Option<tasks::Request>,
    ) -> io::Result<RoomUpdate> {
        let check = pending.or(outgoing.as_ref()).map(|message| message.id);
        let request = Request::Exchange {
            member: self.member,
            after,
            outgoing,
            check,
            readiness,
            task,
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

#[derive(Serialize, Deserialize)]
enum SavedRoom {
    Host {
        invite: Invite,
        key: Vec<u8>,
        owner: Secret,
        expires_at: u64,
    },
    Guest {
        invite: Invite,
        member: Secret,
    },
}

pub struct RoomConnection {
    pub client: RoomClient,
    pub host: Option<(RoomHost, String)>,
    pub room_id: String,
    pub ready: bool,
}

impl RoomConnection {
    pub fn create(ip: Ipv4Addr, directory: &Path) -> io::Result<Self> {
        if !allowed_address(ip) {
            return Err(problem("请选择本机的内网 IPv4 地址"));
        }
        let listener = TcpListener::bind((ip, 0))?;
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec![SERVER_NAME.to_string()]).map_err(other)?;
        let invite = Invite {
            address: SocketAddrV4::new(ip, listener.local_addr()?.port()),
            certificate: cert.der().as_ref().to_vec(),
            token: random()?,
        };
        let owner = random()?;
        let key = signing_key.serialize_der();
        let expires_at = now()? + INVITE_LIFETIME.as_secs();
        let mut sessions = Sessions::open(directory)?;
        sessions.remember(
            &invite.room_id(),
            &SavedRoom::Host {
                invite: invite.clone(),
                key: key.clone(),
                owner,
                expires_at,
            },
        )?;
        let connection = RoomHost::listen(listener, directory, invite, key, owner, expires_at)?;
        sessions.mark_opened(&connection.room_id)?;
        Ok(connection)
    }

    pub fn join(code: &str, directory: &Path) -> io::Result<Self> {
        let invite = Invite::decode(code)?;
        let room_id = invite.room_id();
        let mut sessions = Sessions::open(directory)?;
        if sessions.get::<SavedRoom>(&room_id)?.is_none() {
            // Record the credential before sending it so a lost reply is safe to retry.
            sessions.remember(
                &room_id,
                &SavedRoom::Guest {
                    invite: invite.clone(),
                    member: random()?,
                },
            )?;
        }
        let member = match sessions.get::<SavedRoom>(&room_id)? {
            Some(SavedRoom::Guest { member, .. }) => member,
            _ => return Err(problem("这台电脑已经以主协调端身份保存了该房间")),
        };
        let client = RoomClient::join(&invite, member)?;
        sessions.mark_opened(&room_id)?;
        Ok(Self {
            client,
            host: None,
            room_id,
            ready: true,
        })
    }

    pub fn recent(directory: &Path) -> io::Result<Vec<(String, Member)>> {
        let saved = Sessions::open_existing(directory)?.recent::<SavedRoom>()?;
        Ok(saved
            .into_iter()
            .map(|(id, room)| {
                let member = match room {
                    SavedRoom::Host { .. } => Member::Host,
                    SavedRoom::Guest { .. } => Member::Guest,
                };
                (id, member)
            })
            .collect())
    }

    pub fn resume(directory: &Path, room_id: Option<&str>) -> io::Result<Self> {
        let mut sessions = Sessions::open(directory)?;
        let saved = sessions
            .opened(room_id)?
            .ok_or_else(|| problem("本机没有可继续的对应房间"))?;
        let connection = match saved {
            SavedRoom::Host {
                invite,
                key,
                owner,
                expires_at,
            } => {
                let listener = TcpListener::bind(invite.address).map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("无法监听原来的内网地址和端口，请检查网卡地址与端口占用。{error}"),
                    )
                })?;
                RoomHost::listen(listener, directory, invite, key, owner, expires_at)
            }
            SavedRoom::Guest { invite, member } => Ok(Self {
                client: RoomClient::new(&invite, member)?,
                host: None,
                room_id: invite.room_id(),
                ready: false,
            }),
        }?;
        sessions.mark_opened(&connection.room_id)?;
        Ok(connection)
    }
}

impl RoomHost {
    fn listen(
        listener: TcpListener,
        directory: &Path,
        invite: Invite,
        key: Vec<u8>,
        owner: Secret,
        expires_at: u64,
    ) -> io::Result<RoomConnection> {
        listener.set_nonblocking(true)?;
        let tls =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(other)?
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(invite.certificate.clone())],
                    PrivatePkcs8KeyDer::from(key).into(),
                )
                .map_err(other)?;
        let client = RoomClient::new(&invite, owner)?;
        let code = invite.encode()?;
        let room_id = invite.room_id();
        let rooms = directory.join("rooms");
        std::fs::create_dir_all(&rooms)?;
        let mut store =
            block_on(open_room(&rooms.join(format!("{room_id}.sqlite3")))).map_err(other)?;
        let guest = block_on(
            sqlx::query_scalar::<_, Vec<u8>>("SELECT fingerprint FROM room_guest WHERE slot = 1")
                .fetch_optional(&mut store),
        )
        .map_err(other)?;
        let readiness = block_on(
            sqlx::query_scalar::<_, Vec<u8>>("SELECT state FROM room_readiness WHERE slot = 1")
                .fetch_optional(&mut store),
        )
        .map_err(other)?
        .map(|state| serde_json::from_slice(&state))
        .transpose()
        .map_err(other)?
        .unwrap_or_default();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let mut room = ServerRoom {
            store,
            owner,
            invitation: invite.token,
            guest,
            expires_at,
            readiness,
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
        Ok(RoomConnection {
            client,
            host: Some((Self { stop }, code)),
            room_id,
            ready: true,
        })
    }
}

struct ServerRoom {
    store: SqliteConnection,
    owner: Secret,
    invitation: Secret,
    guest: Option<Vec<u8>>,
    expires_at: u64,
    readiness: readiness::State,
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
                let fingerprint = digest(&SHA256, &member);
                if let Some(guest) = &self.guest {
                    return if bool::from(guest.ct_eq(fingerprint.as_ref())) {
                        Reply::Joined
                    } else {
                        Reply::Rejected("该邀请已被使用".into())
                    };
                }
                let Ok(current) = now() else {
                    return Reply::Rejected("主机系统时间异常".into());
                };
                if current >= self.expires_at {
                    return Reply::Rejected("邀请已过期，请由主协调端重新创建房间".into());
                }
                if block_on(
                    sqlx::query("INSERT INTO room_guest (slot, fingerprint) VALUES (1, ?1)")
                        .bind(fingerprint.as_ref())
                        .execute(&mut self.store),
                )
                .is_err()
                {
                    return Reply::Rejected("主机暂时无法保存接入身份".into());
                }
                self.guest = Some(fingerprint.as_ref().to_vec());
                Reply::Joined
            }
            Request::Exchange {
                member,
                after,
                outgoing,
                check,
                readiness,
                task,
            } => {
                let role =
                    if bool::from(self.owner.ct_eq(&member)) {
                        Member::Host
                    } else if self.guest.as_ref().is_some_and(|guest| {
                        bool::from(guest.ct_eq(digest(&SHA256, &member).as_ref()))
                    }) {
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
                let advertised = readiness.and_then(|request| request.binding);
                let readiness = match readiness
                    .map(|request| self.update_readiness(role, request))
                    .transpose()
                {
                    Ok(update) => update,
                    Err(_) => return Reply::Rejected("主机暂时无法保存接单状态".into()),
                };
                let task = if let Some(task) = task {
                    let Ok(timestamp) = now() else {
                        return Reply::Rejected("主机系统时间异常".into());
                    };
                    match block_on(tasks::apply(
                        &mut self.store,
                        role,
                        task.action,
                        self.readiness.verified_bindings(),
                        advertised.or(task.binding),
                        timestamp,
                    )) {
                        Ok(update) => Some(update),
                        Err(_) => return Reply::Rejected("主机暂时无法保存或读取任务".into()),
                    }
                } else {
                    None
                };
                match block_on(exchange_room(&mut self.store, role, after, outgoing, check)) {
                    Ok((messages, confirmed)) => Reply::Update(RoomUpdate {
                        messages,
                        confirmed,
                        guest_joined: self.guest.is_some(),
                        readiness,
                        task,
                    }),
                    Err(_) => Reply::Rejected("主机暂时无法保存或读取房间消息".into()),
                }
            }
        }
    }

    fn update_readiness(
        &mut self,
        role: Member,
        request: readiness::Request,
    ) -> io::Result<readiness::Update> {
        let mut state = self.readiness.clone();
        let update = state.apply(role, request, now()?)?;
        if state != self.readiness {
            block_on(
                sqlx::query(
                    "INSERT INTO room_readiness (slot, state) VALUES (1, ?1)
                     ON CONFLICT(slot) DO UPDATE SET state = excluded.state",
                )
                .bind(serde_json::to_vec(&state).map_err(other)?)
                .execute(&mut self.store),
            )
            .map_err(other)?;
            self.readiness = state;
        }
        Ok(update)
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
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_guest (
            slot INTEGER PRIMARY KEY CHECK (slot = 1),
            fingerprint BLOB NOT NULL
        )",
    )
    .execute(&mut store)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS room_readiness (slot INTEGER PRIMARY KEY, state BLOB NOT NULL)",
    )
    .execute(&mut store)
    .await?;
    tasks::initialize(&mut store).await?;
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

fn now() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(other)
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
    fn restores_both_members_and_chat_after_host_shutdown() {
        let owner_data = tempfile::tempdir().unwrap();
        let guest_data = tempfile::tempdir().unwrap();
        let other_data = tempfile::tempdir().unwrap();
        let host = RoomConnection::create(Ipv4Addr::LOCALHOST, owner_data.path()).unwrap();
        let code = host.host.as_ref().unwrap().1.clone();
        let guest = RoomConnection::join(&code, guest_data.path()).unwrap();
        guest
            .client
            .exchange(
                0,
                Some(Outgoing::new("之前发的中文消息".into()).unwrap()),
                None,
            )
            .unwrap();
        let address = Invite::decode(&code).unwrap().address;
        let id = host.room_id.clone();
        let owner_check = readiness::Request {
            binding: Some([1; 32]),
            attempt: [1; 16],
            retry: false,
            result: None,
        };
        let guest_check = readiness::Request {
            binding: Some([2; 32]),
            attempt: [2; 16],
            ..owner_check
        };
        host.client
            .exchange_with_readiness(0, None, None, Some(owner_check))
            .unwrap();
        let challenge = guest
            .client
            .exchange_with_readiness(0, None, None, Some(guest_check))
            .unwrap()
            .readiness
            .unwrap()
            .challenge
            .unwrap();
        let passed = readiness::Request {
            result: Some(readiness::Report {
                challenge,
                answer: Ok(challenge),
            }),
            ..guest_check
        };
        guest
            .client
            .exchange_with_readiness(0, None, None, Some(passed))
            .unwrap();
        drop(guest);
        drop(host);

        let deadline = Instant::now() + REQUEST_TIMEOUT;
        while TcpStream::connect_timeout(&address.into(), Duration::from_millis(50)).is_ok() {
            assert!(Instant::now() < deadline, "主机应在关闭后释放监听");
            std::thread::sleep(Duration::from_millis(20));
        }
        let newer = RoomConnection::create(Ipv4Addr::LOCALHOST, owner_data.path()).unwrap();
        let newer_code = &newer.host.as_ref().unwrap().1;
        let _newer_guest = RoomConnection::join(newer_code, guest_data.path()).unwrap();
        assert_eq!(
            RoomConnection::recent(owner_data.path()).unwrap(),
            [
                (newer.room_id.clone(), Member::Host),
                (id.clone(), Member::Host)
            ]
        );
        assert_eq!(
            RoomConnection::recent(guest_data.path()).unwrap(),
            [
                (newer.room_id.clone(), Member::Guest),
                (id.clone(), Member::Guest)
            ]
        );
        assert!(Sessions::has_last(guest_data.path()).unwrap());
        let guest = RoomConnection::resume(guest_data.path(), Some(&id)).unwrap();
        assert!(!guest.ready);
        let host = RoomConnection::resume(owner_data.path(), Some(&id)).unwrap();
        assert_eq!(host.room_id, id);
        assert_eq!(guest.room_id, id);
        assert_eq!(host.host.as_ref().unwrap().1, code);
        assert_eq!(RoomConnection::recent(owner_data.path()).unwrap()[0].0, id);
        assert_eq!(
            RoomConnection::resume(guest_data.path(), None)
                .unwrap()
                .room_id,
            id
        );
        assert!(RoomConnection::join(&code, other_data.path()).is_err());
        assert!(!Sessions::has_last(other_data.path()).unwrap());
        assert!(
            RoomConnection::recent(other_data.path())
                .unwrap()
                .is_empty()
        );
        assert!(RoomConnection::resume(other_data.path(), Some(&id)).is_err());
        let restarted = readiness::Request {
            attempt: [3; 16],
            ..guest_check
        };
        let ready = guest
            .client
            .exchange_with_readiness(0, None, None, Some(restarted))
            .unwrap()
            .readiness
            .unwrap();
        assert_eq!(ready.guest, readiness::Status::Passed);
        assert_eq!(ready.challenge, None);

        let history = guest.client.exchange(0, None, None).unwrap();
        assert!(history.guest_joined);
        assert_eq!(history.messages.len(), 1);
        assert_eq!(history.messages[0].body, "之前发的中文消息");
        let after = history.messages[0].id;
        host.client
            .exchange(
                after,
                Some(Outgoing::new("重启后的回复".into()).unwrap()),
                None,
            )
            .unwrap();
        assert_eq!(
            guest.client.exchange(after, None, None).unwrap().messages[0].body,
            "重启后的回复"
        );
    }

    #[test]
    fn two_members_exchange_and_store_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let host = RoomConnection::create(Ipv4Addr::LOCALHOST, directory.path()).unwrap();
        let owner = &host.client;
        let code = &host.host.as_ref().unwrap().1;
        let invite = Invite::decode(code).unwrap();
        let guest_identity = random().unwrap();
        let guest = RoomClient::join(&invite, guest_identity).unwrap();
        assert!(RoomClient::join(&invite, random().unwrap()).is_err());
        assert!(RoomClient::join(&invite, guest_identity).is_ok());

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

        let stored = directory
            .path()
            .join("rooms")
            .join(format!("{}.sqlite3", host.room_id));
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
        let a = tempfile::tempdir_in(directory.path()).unwrap();
        let b = tempfile::tempdir_in(directory.path()).unwrap();
        let host = RoomConnection::create(Ipv4Addr::LOCALHOST, a.path()).unwrap();
        let other = RoomConnection::create(Ipv4Addr::LOCALHOST, b.path()).unwrap();
        let mut invite = Invite::decode(&host.host.as_ref().unwrap().1).unwrap();
        let wrong = RoomClient::new(&invite, random().unwrap()).unwrap();
        assert!(wrong.exchange(0, None, None).is_err());
        invite.token = random().unwrap();
        assert!(RoomClient::join(&invite, random().unwrap()).is_err());
        invite.certificate = Invite::decode(&other.host.as_ref().unwrap().1)
            .unwrap()
            .certificate;
        assert!(RoomClient::join(&invite, random().unwrap()).is_err());
    }
}
