//! Chat P2P na rede local, com interface de terminal (TUI).
//!
//! - Cada instância é ao mesmo tempo "servidor" e "cliente" (sem servidor central).
//! - Descoberta automática na LAN via UDP broadcast + conexão TCP entre os pares.
//! - As mensagens são repassadas (gossip) entre os pares, então mesmo uma
//!   conexão manual (/conectar) forma uma rede onde todos se enxergam.
//! - A interface separa a área de mensagens da caixa de digitação: o que você
//!   está escrevendo nunca é interrompido por mensagens que chegam.
//!
//! Uso:  cargo run --release -- [nome] [porta] [ip:porta ...]
//e
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::io::{self, BufRead, BufReader, Write};
use std::net::{Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
    MouseEventKind,
};
use ratatui::crossterm::execute;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};
use ratatui::{DefaultTerminal, Frame};

const DEFAULT_PORT: u16 = 3942;
const DISCOVERY_PORT: u16 = 3943;
const DISCOVERY_MAGIC: &str = "AIRMINAS1";

// ───────────────────────── Estado compartilhado ─────────────────────────

struct Peer {
    name: String,
    stream: TcpStream, // clone usado apenas para escrita
}

struct State {
    peers: HashMap<u64, Peer>,
    seen: HashSet<(u64, u64)>, // (origem, seq) das mensagens já vistas
    seq: u64,
}

/// Eventos enviados das threads de rede para a interface.
enum Evt {
    Chat { name: String, text: String },
    System(String),
}

struct Ctx {
    id: u64,
    name: String,
    tcp_port: u16,
    state: Mutex<State>,
    tx: Sender<Evt>,
}

impl Ctx {
    fn sys(&self, msg: String) {
        let _ = self.tx.send(Evt::System(msg));
    }
}

// ───────────────────────────── Rede ─────────────────────────────

/// Envia uma linha para todos os pares (exceto `except`). Remove os que falharem.
fn send_all(ctx: &Ctx, line: &str, except: Option<u64>) {
    let mut dead = Vec::new();
    {
        let mut st = ctx.state.lock().unwrap();
        for (id, peer) in st.peers.iter_mut() {
            if Some(*id) == except {
                continue;
            }
            if peer.stream.write_all(line.as_bytes()).is_err() {
                dead.push(*id);
            }
        }
    }
    for id in dead {
        drop_peer(ctx, id);
    }
}

fn drop_peer(ctx: &Ctx, id: u64) {
    let removed = ctx.state.lock().unwrap().peers.remove(&id);
    if let Some(p) = removed {
        let _ = p.stream.shutdown(Shutdown::Both);
        ctx.sys(format!("{} saiu da rede.", p.name));
    }
}

/// Cuida de uma conexão (entrada ou saída): handshake + leitura de mensagens.
fn handle_conn(ctx: Arc<Ctx>, stream: TcpStream) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));

    let Ok(mut writer) = stream.try_clone() else { return };
    let hello = format!("HELLO\t{}\t{}\n", ctx.id, ctx.name);
    if writer.write_all(hello.as_bytes()).is_err() {
        return;
    }

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let parts: Vec<&str> = line.trim_end().splitn(3, '\t').collect();
    if parts.len() != 3 || parts[0] != "HELLO" {
        return;
    }
    let Ok(peer_id) = parts[1].parse::<u64>() else { return };
    let peer_name = parts[2].to_string();
    if peer_id == ctx.id {
        return; // conectou em si mesmo
    }

    {
        let mut st = ctx.state.lock().unwrap();
        if st.peers.contains_key(&peer_id) {
            return; // já conectado a este par
        }
        st.peers.insert(
            peer_id,
            Peer {
                name: peer_name.clone(),
                stream: writer,
            },
        );
    }
    let _ = reader.get_ref().set_read_timeout(None);
    ctx.sys(format!("{peer_name} entrou na rede."));

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let l = line.trim_end_matches(['\r', '\n']);
        let p: Vec<&str> = l.splitn(5, '\t').collect();
        if p.len() == 5 && p[0] == "MSG" {
            let (Ok(origin), Ok(seq)) = (p[1].parse::<u64>(), p[2].parse::<u64>()) else {
                continue;
            };
            if origin == ctx.id {
                continue;
            }
            {
                let mut st = ctx.state.lock().unwrap();
                if !st.seen.insert((origin, seq)) {
                    continue; // duplicada
                }
            }
            let _ = ctx.tx.send(Evt::Chat {
                name: p[3].to_string(),
                text: p[4].to_string(),
            });
            // Gossip: repassa para os demais pares
            send_all(&ctx, &format!("{l}\n"), Some(peer_id));
        }
    }
    drop_peer(&ctx, peer_id);
}

fn dial(ctx: Arc<Ctx>, addr: SocketAddr) {
    thread::spawn(move || match TcpStream::connect_timeout(&addr, Duration::from_secs(3)) {
        Ok(s) => handle_conn(ctx, s),
        Err(e) => ctx.sys(format!("Falha ao conectar em {addr}: {e}")),
    });
}

fn start_listener(ctx: Arc<Ctx>, listener: TcpListener) {
    thread::spawn(move || {
        for conn in listener.incoming().flatten() {
            let ctx = ctx.clone();
            thread::spawn(move || handle_conn(ctx, conn));
        }
    });
}

/// Descoberta na LAN: anuncia a si mesmo e conecta a quem anunciar.
/// Para evitar conexões duplicadas, só disca quem tem o menor id.
fn start_discovery(ctx: Arc<Ctx>) -> io::Result<()> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_broadcast(true)?;
    sock.bind(&SocketAddr::from((Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT)).into())?;
    let udp: UdpSocket = sock.into();

    // Anunciante
    let sender = udp.try_clone()?;
    let announce = format!("{DISCOVERY_MAGIC}\t{}\t{}", ctx.id, ctx.tcp_port);
    thread::spawn(move || loop {
        let _ = sender.send_to(
            announce.as_bytes(),
            (Ipv4Addr::BROADCAST, DISCOVERY_PORT),
        );
        thread::sleep(Duration::from_secs(2));
    });

    // Ouvinte
    thread::spawn(move || {
        let mut buf = [0u8; 256];
        loop {
            let Ok((n, src)) = udp.recv_from(&mut buf) else { continue };
            let msg = String::from_utf8_lossy(&buf[..n]);
            let p: Vec<&str> = msg.trim().split('\t').collect();
            if p.len() != 3 || p[0] != DISCOVERY_MAGIC {
                continue;
            }
            let (Ok(peer_id), Ok(port)) = (p[1].parse::<u64>(), p[2].parse::<u16>()) else {
                continue;
            };
            if peer_id == ctx.id || ctx.id > peer_id {
                continue; // o outro lado é quem disca
            }
            if ctx.state.lock().unwrap().peers.contains_key(&peer_id) {
                continue;
            }
            dial(ctx.clone(), SocketAddr::new(src.ip(), port));
        }
    });
    Ok(())
}

// ───────────────────────────── Interface ─────────────────────────────

#[derive(Clone, Copy)]
enum Kind {
    Me,
    Other,
    System,
}

struct App {
    msgs: Vec<(Kind, String)>,
    input: String,
    scroll: usize, // linhas roladas para cima (0 = colado no fim)
    width: usize,  // largura útil da área de mensagens (atualizada no draw)
    page: usize,   // altura útil da área de mensagens (atualizada no draw)
}

impl App {
    /// Adiciona uma mensagem. Mantém a tela parada se o usuário estiver
    /// lendo mensagens antigas.
    fn push(&mut self, kind: Kind, text: String) {
        if self.scroll > 0 {
            self.scroll = self.scroll.saturating_add(wrap(&text, self.width).len());
        }
        self.msgs.push((kind, text));
    }
}

fn wrap(s: &str, width: usize) -> Vec<String> {
    let chars: Vec<char> = s.chars().collect();
    if chars.is_empty() {
        return vec![String::new()];
    }
    chars
        .chunks(width.max(1))
        .map(|c| c.iter().collect())
        .collect()
}

fn draw(f: &mut Frame, app: &mut App, ctx: &Ctx) {
    let areas = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(3),
        Constraint::Length(3),
    ])
    .split(f.area());

    // Cabeçalho: quem está na sala
    let peers: Vec<String> = ctx
        .state
        .lock()
        .unwrap()
        .peers
        .values()
        .map(|p| p.name.clone())
        .collect();
    let header = if peers.is_empty() {
        "Procurando pares na rede...".to_string()
    } else {
        format!("Conectados ({}): {}", peers.len(), peers.join(", "))
    };
    f.render_widget(
        Paragraph::new(header).block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" P2P Chat — {} (porta {}) ", ctx.name, ctx.tcp_port)),
        ),
        areas[0],
    );

    // Área de mensagens (separada da digitação)
    let inner_w = areas[1].width.saturating_sub(2) as usize;
    let inner_h = areas[1].height.saturating_sub(2) as usize;
    let mut rows: Vec<Line> = Vec::new();
    for (kind, text) in &app.msgs {
        let style = match kind {
            Kind::Me => Style::default().fg(Color::Green),
            Kind::Other => Style::default().fg(Color::Cyan),
            Kind::System => Style::default()
                .fg(Color::DarkGray)
                .add_modifier(Modifier::ITALIC),
        };
        for part in wrap(text, inner_w) {
            rows.push(Line::styled(part, style));
        }
    }
    let max_scroll = rows.len().saturating_sub(inner_h);
    let scroll = app.scroll.min(max_scroll);
    app.scroll = scroll;
    app.width = inner_w;
    app.page = inner_h;
    let end = rows.len() - scroll;
    let start = end.saturating_sub(inner_h);
    let title = if scroll > 0 {
        format!(" Mensagens — {scroll} linhas acima do fim (End volta) ")
    } else {
        " Mensagens ".to_string()
    };
    f.render_widget(
        Paragraph::new(rows[start..end].to_vec())
            .block(Block::default().borders(Borders::ALL).title(title)),
        areas[1],
    );

    // Caixa de digitação
    let in_w = areas[2].width.saturating_sub(3) as usize;
    let chars: Vec<char> = app.input.chars().collect();
    let visible: String = chars[chars.len().saturating_sub(in_w)..].iter().collect();
    let vis_len = visible.chars().count() as u16;
    f.render_widget(
        Paragraph::new(visible).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Enter envia · ↑↓ PgUp PgDn rolam · /sair "),
        ),
        areas[2],
    );
    f.set_cursor_position((areas[2].x + 1 + vis_len, areas[2].y + 1));
}

fn send_chat(ctx: &Ctx, text: &str) {
    let text = text.replace(['\t', '\n', '\r'], " ");
    let seq = {
        let mut st = ctx.state.lock().unwrap();
        st.seq += 1;
        let seq = st.seq;
        st.seen.insert((ctx.id, seq));
        seq
    };
    let line = format!("MSG\t{}\t{}\t{}\t{}\n", ctx.id, seq, ctx.name, text);
    send_all(ctx, &line, None);
}

fn run(term: &mut DefaultTerminal, ctx: Arc<Ctx>, rx: Receiver<Evt>) -> io::Result<()> {
    let mut msgs: Vec<(Kind, String)> = Vec::new();
    msgs.push((
        Kind::System,
        format!(
            "Bem-vindo, {}! Escutando na porta {}. Aguardando outros pares...",
            ctx.name, ctx.tcp_port
        ),
    ));
    let mut app = App {
        msgs,
        input: String::new(),
        scroll: 0,
        width: 80,
        page: 10,
    };

    loop {
        while let Ok(ev) = rx.try_recv() {
            match ev {
                Evt::Chat { name, text } => app.push(Kind::Other, format!("[{name}]: {text}")),
                Evt::System(s) => app.push(Kind::System, format!("* {s}")),
            }
        }

        term.draw(|f| draw(f, &mut app, &ctx))?;

        if !event::poll(Duration::from_millis(50))? {
            continue;
        }
        let key = match event::read()? {
            Event::Key(k) => k,
            Event::Mouse(m) => {
                match m.kind {
                    MouseEventKind::ScrollUp => app.scroll = app.scroll.saturating_add(3),
                    MouseEventKind::ScrollDown => app.scroll = app.scroll.saturating_sub(3),
                    _ => {}
                }
                continue;
            }
            _ => continue,
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
            KeyCode::Esc => break,
            KeyCode::Char(c) => app.input.push(c),
            KeyCode::Backspace => {
                app.input.pop();
            }
            KeyCode::Up => app.scroll = app.scroll.saturating_add(1),
            KeyCode::Down => app.scroll = app.scroll.saturating_sub(1),
            KeyCode::PageUp => app.scroll = app.scroll.saturating_add(app.page.max(2) - 1),
            KeyCode::PageDown => app.scroll = app.scroll.saturating_sub(app.page.max(2) - 1),
            KeyCode::Home => app.scroll = usize::MAX,
            KeyCode::End => app.scroll = 0,
            KeyCode::Enter => {
                let text = app.input.trim().to_string();
                app.input.clear();
                if text.is_empty() {
                    continue;
                }
                if text.eq_ignore_ascii_case("/sair") || text.eq_ignore_ascii_case("sair") {
                    break;
                }
                if let Some(addr) = text.strip_prefix("/conectar ") {
                    match addr.trim().parse::<SocketAddr>() {
                        Ok(a) => {
                            app.push(Kind::System, format!("* Conectando em {a}..."));
                            dial(ctx.clone(), a);
                        }
                        Err(_) => {
                            app.push(Kind::System, "* Endereço inválido. Use ip:porta".into())
                        }
                    }
                    continue;
                }
                app.push(Kind::Me, format!("[Você]: {text}"));
                app.scroll = 0;
                send_chat(&ctx, &text);
            }
            _ => {}
        }
    }
    Ok(())
}

// ───────────────────────────── main ─────────────────────────────

fn main() -> io::Result<()> {
    let mut name: Option<String> = None;
    let mut port = DEFAULT_PORT;
    let mut manual_peers: Vec<SocketAddr> = Vec::new();
    for (i, arg) in std::env::args().skip(1).enumerate() {
        if i == 0 {
            name = Some(arg);
        } else if let Ok(p) = arg.parse::<u16>() {
            port = p;
        } else if let Ok(a) = arg.parse::<SocketAddr>() {
            manual_peers.push(a);
        }
    }
    let name = match name {
        Some(n) => n,
        None => {
            print!("Digite seu nome/apelido para o chat: ");
            io::stdout().flush()?;
            let mut s = String::new();
            io::stdin().read_line(&mut s)?;
            s
        }
    };
    let name = name.trim().replace(['\t', '\n', '\r'], " ");
    let name = if name.is_empty() { "anonimo".to_string() } else { name };

    // Tenta a porta pedida; se estiver ocupada (ex.: 2 instâncias na mesma máquina), tenta as próximas
    let (listener, tcp_port) = (0..20u16)
        .find_map(|d| {
            let p = port.checked_add(d)?;
            TcpListener::bind(("0.0.0.0", p)).ok().map(|l| (l, p))
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::AddrInUse, "nenhuma porta TCP livre"))?;

    let (tx, rx) = mpsc::channel();
    let ctx = Arc::new(Ctx {
        id: RandomState::new().build_hasher().finish(),
        name,
        tcp_port,
        state: Mutex::new(State {
            peers: HashMap::new(),
            seen: HashSet::new(),
            seq: 0,
        }),
        tx,
    });

    start_listener(ctx.clone(), listener);
    if let Err(e) = start_discovery(ctx.clone()) {
        ctx.sys(format!(
            "Descoberta automática indisponível ({e}). Use /conectar ip:porta."
        ));
    }
    for a in manual_peers {
        dial(ctx.clone(), a);
    }

    let mut term = ratatui::init();
    let _ = execute!(io::stdout(), EnableMouseCapture);
    let result = run(&mut term, ctx, rx);
    let _ = execute!(io::stdout(), DisableMouseCapture);
    ratatui::restore();
    result
}